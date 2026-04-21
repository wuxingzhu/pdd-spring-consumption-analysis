# -*- coding: utf-8 -*-
"""
拼多多春季消费分析 - 数据清洗脚本
处理源数据中的：空值、异常值、格式错误、重复数据、逻辑错误
"""

import pandas as pd
import numpy as np
import os
import re


# ==================== 1. 清洗用户表 ====================
def clean_users(df):
    """清洗用户数据"""
    print("\n【清洗用户表】")
    print(f"  清洗前: {len(df)} 行")

    # 1.1 删除 user_id 重复的行（如果有）
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['user_id'])
    print(f"  去重后: {len(df)} 行 (删除 {before_dedup - len(df)} 行)")

    # 1.2 处理 city（城市）
    # 空值用 '未知' 填充
    df['city'] = df['city'].fillna('未知')

    # 1.3 处理 age（年龄）
    # 删除异常值：age < 0 或 age > 100
    before_filter = len(df)
    df = df[(df['age'] >= 18) & (df['age'] <= 65) | (df['age'].isna())]
    print(f"  删除年龄异常值: {before_filter - len(df)} 行")
    # 空值用中位数填充
    median_age = df['age'].median()
    df['age'] = df['age'].fillna(median_age)

    # 1.4 处理 gender（性别）
    # 空值用 '未知' 填充
    df['gender'] = df['gender'].fillna('未知')
    # 标准化性别值
    df['gender'] = df['gender'].map({
        'male': '男', 'female': '女', '未知': '未知'
    }).fillna('未知')

    # 1.5 处理 membership_level（会员等级）
    # 空值用 '普通' 填充，异常值统一为 '普通'
    df['membership_level'] = df['membership_level'].fillna('普通')
    valid_levels = ['普通', '黄金', '铂金']
    df['membership_level'] = df['membership_level'].apply(
        lambda x: x if x in valid_levels else '普通'
    )

    # 1.6 处理 registration_date（注册日期）
    # 删除格式错误的行（无法解析的日期）
    def parse_date(date_str):
        if pd.isna(date_str):
            return None
        try:
            # 尝试多种格式
            for fmt in ['%Y-%m-%d', '%Y/%m/%d']:
                try:
                    return pd.to_datetime(date_str, format=fmt)
                except:
                    pass
            # 如果都失败，尝试自动解析
            return pd.to_datetime(date_str)
        except:
            return None

    df['registration_date_parsed'] = df['registration_date'].apply(parse_date)
    before_filter = len(df)
    df = df[df['registration_date_parsed'].notna()]
    print(f"  删除格式错误日期: {before_filter - len(df)} 行")
    df['registration_date'] = df['registration_date_parsed']
    df = df.drop(columns=['registration_date_parsed'])

    # 1.7 处理 phone_number（手机号）
    def clean_phone(phone):
        if pd.isna(phone):
            return None
        # 只保留数字
        phone_str = str(phone)
        digits = re.sub(r'\D', '', phone_str)
        # 有效手机号：11位，以1开头
        if len(digits) == 11 and digits.startswith('1'):
            return digits
        return None

    df['phone_number'] = df['phone_number'].apply(clean_phone)

    # 1.8 处理 last_login_date（最后登录日期）
    df['last_login_date'] = pd.to_datetime(df['last_login_date'], errors='coerce')

    # 1.9 处理 user_source（用户来源）
    df['user_source'] = df['user_source'].fillna('未知')
    df['user_source'] = df['user_source'].replace('', '未知')

    print(f"  清洗后: {len(df)} 行")
    return df


# ==================== 2. 清洗商品表 ====================
def clean_products(df):
    """清洗商品数据"""
    print("\n【清洗商品表】")
    print(f"  清洗前: {len(df)} 行")

    # 2.1 删除 product_id 重复的行
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['product_id'])
    print(f"  去重后: {len(df)} 行 (删除 {before_dedup - len(df)} 行)")

    # 2.2 处理 price（价格）
    # 删除价格为负或为0的行
    before_filter = len(df)
    df = df[(df['price'] > 0) | (df['price'].isna())]
    print(f"  删除价格为负/零: {before_filter - len(df)} 行")
    # 空值用同类目均价填充
    for category in df['category'].unique():
        median_price = df[df['category'] == category]['price'].median()
        df.loc[(df['category'] == category) & (df['price'].isna()), 'price'] = median_price

    # 2.3 处理 original_price（原价）
    # 空值用 price * 1.2 填充
    df['original_price'] = df['original_price'].fillna(df['price'] * 1.2)
    # 删除原价低于拼单价的行
    df = df[df['original_price'] >= df['price']]

    # 2.4 处理 brand_type（品牌类型）
    df['brand_type'] = df['brand_type'].fillna('未知')
    valid_brands = ['国货', '国际']
    df['brand_type'] = df['brand_type'].apply(
        lambda x: x if x in valid_brands else '未知'
    )

    # 2.5 处理 sales_volume（销量）
    # 删除负数销量
    before_filter = len(df)
    df = df[df['sales_volume'] >= 0 | (df['sales_volume'].isna())]
    print(f"  删除销量负值: {before_filter - len(df)} 行")
    df['sales_volume'] = df['sales_volume'].fillna(0).astype(int)

    # 2.6 处理 rating（评分）
    # 删除超出1-5范围的评分
    before_filter = len(df)
    df = df[(df['rating'] >= 1) & (df['rating'] <= 5) | (df['rating'].isna())]
    print(f"  删除评分异常值: {before_filter - len(df)} 行")
    df['rating'] = df['rating'].fillna(df['rating'].median())

    # 2.7 处理 warehouse_city（仓库城市）
    df['warehouse_city'] = df['warehouse_city'].fillna('未知')
    df['warehouse_city'] = df['warehouse_city'].replace('', '未知')

    # 2.8 处理 product_description（商品描述）
    # 删除特殊字符（保留中文、英文、数字）
    def clean_description(desc):
        if pd.isna(desc):
            return ''
        # 移除非中英文数字的字符
        cleaned = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9]', '', str(desc))
        return cleaned if cleaned else ''

    df['product_description'] = df['product_description'].apply(clean_description)

    print(f"  清洗后: {len(df)} 行")
    return df


# ==================== 3. 清洗订单表 ====================
def clean_orders(df, users_df, products_df):
    """清洗订单数据"""
    print("\n【清洗订单表】")
    print(f"  清洗前: {len(df)} 行")

    # 3.1 删除重复订单
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['order_id'])
    print(f"  去重后: {len(df)} 行 (删除 {before_dedup - len(df)} 行)")

    # 3.2 删除 user_id 不在用户表中的订单
    valid_user_ids = set(users_df['user_id'])
    before_filter = len(df)
    df = df[df['user_id'].isin(valid_user_ids)]
    print(f"  删除无效用户订单: {before_filter - len(df)} 行")

    # 3.3 删除 product_id 不在商品表中的订单
    valid_product_ids = set(products_df['product_id'])
    before_filter = len(df)
    df = df[df['product_id'].isin(valid_product_ids)]
    print(f"  删除无效商品订单: {before_filter - len(df)} 行")

    # 3.4 处理 order_time（订单时间）
    df['order_time'] = pd.to_datetime(df['order_time'], errors='coerce')
    # 删除时间无效的行
    before_filter = len(df)
    df = df[df['order_time'].notna()]
    print(f"  删除时间无效订单: {before_filter - len(df)} 行")

    # 3.5 处理 payment_time（支付时间）
    df['payment_time'] = pd.to_datetime(df['payment_time'], errors='coerce')

    # 3.6 处理 quantity（数量）
    # 删除数量为负的行
    before_filter = len(df)
    df = df[df['quantity'] > 0 | (df['quantity'].isna())]
    print(f"  删除数量负值: {before_filter - len(df)} 行")
    df['quantity'] = df['quantity'].fillna(1).astype(int)

    # 3.7 处理 unit_price（单价）
    # 删除单价为负的行
    before_filter = len(df)
    df = df[df['unit_price'] > 0 | (df['unit_price'].isna())]
    print(f"  删除单价负值: {before_filter - len(df)} 行")
    # 空值用商品表中的价格填充
    price_map = products_df.set_index('product_id')['price'].to_dict()
    df['unit_price'] = df.apply(
        lambda row: price_map.get(row['product_id'], row['unit_price'])
        if pd.isna(row['unit_price']) else row['unit_price'],
        axis=1
    )

    # 3.8 处理 coupon_discount（优惠券）
    df['coupon_discount'] = df['coupon_discount'].fillna(0)
    # 将负数优惠券改为0
    df['coupon_discount'] = df['coupon_discount'].apply(lambda x: max(x, 0))

    # 3.9 处理 total_amount（总金额）
    # 删除总金额为负的行
    before_filter = len(df)
    df = df[df['total_amount'] > 0 | (df['total_amount'].isna())]
    print(f"  删除金额负值: {before_filter - len(df)} 行")

    # 空值重新计算
    def calc_amount(row):
        if pd.isna(row['total_amount']) or row['total_amount'] <= 0:
            amount = row['quantity'] * row['unit_price'] - row['coupon_discount']
            return max(amount, 0)
        return row['total_amount']

    df['total_amount'] = df.apply(calc_amount, axis=1)

    # 3.10 处理逻辑错误：拼团订单但分享次数为0
    # 对于 is_group_order = True 但 share_count = 0 的订单，修正 share_count 为 1
    mask = (df['is_group_order'] == True) & (df['share_count'] == 0)
    df.loc[mask, 'share_count'] = 1
    print(f"  修正拼团逻辑错误: {mask.sum()} 行")

    # 3.11 处理 logistics_status（物流状态）
    df['logistics_status'] = df['logistics_status'].fillna('未知')
    df['logistics_status'] = df['logistics_status'].replace('', '未知')

    # 3.12 处理 refund_status（退款状态）
    df['refund_status'] = df['refund_status'].fillna('无')
    df['refund_status'] = df['refund_status'].replace('', '无')

    # 3.13 处理 device_type（设备类型）
    df['device_type'] = df['device_type'].fillna('未知')
    df['device_type'] = df['device_type'].replace('', '未知')

    print(f"  清洗后: {len(df)} 行")
    return df


# ==================== 4. 主函数 ====================
def main():
    # 路径设置
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(current_dir)
    raw_dir = os.path.join(project_dir, 'data', 'raw')
    processed_dir = os.path.join(project_dir, 'data', 'processed')

    os.makedirs(processed_dir, exist_ok=True)

    print("=" * 60)
    print("开始数据清洗...")
    print("=" * 60)

    # 读取原始数据
    print("\n读取原始数据...")
    users_raw = pd.read_csv(os.path.join(raw_dir, 'users.csv'), encoding='utf-8-sig')
    products_raw = pd.read_csv(os.path.join(raw_dir, 'products.csv'), encoding='utf-8-sig')
    orders_raw = pd.read_csv(os.path.join(raw_dir, 'orders.csv'), encoding='utf-8-sig')

    print(f"  用户表: {len(users_raw)} 行")
    print(f"  商品表: {len(products_raw)} 行")
    print(f"  订单表: {len(orders_raw)} 行")

    # 清洗
    users_clean = clean_users(users_raw)
    products_clean = clean_products(products_raw)
    orders_clean = clean_orders(orders_raw, users_clean, products_clean)

    # 保存清洗后的数据
    users_clean.to_csv(os.path.join(processed_dir, 'users_clean.csv'), index=False, encoding='utf-8-sig')
    products_clean.to_csv(os.path.join(processed_dir, 'products_clean.csv'), index=False, encoding='utf-8-sig')
    orders_clean.to_csv(os.path.join(processed_dir, 'orders_clean.csv'), index=False, encoding='utf-8-sig')

    print("\n" + "=" * 60)
    print("数据清洗完成！")
    print(f"清洗后文件保存路径: {processed_dir}")
    print("=" * 60)

    # 最终统计
    print("\n【最终数据统计】")
    print(f"  用户数: {len(users_clean)}")
    print(f"  商品数: {len(products_clean)}")
    print(f"  订单数: {len(orders_clean)}")
    print(f"  总交易额: ¥{orders_clean['total_amount'].sum():,.2f}")


if __name__ == '__main__':
    main()