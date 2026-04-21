# -*- coding: utf-8 -*-
"""
拼多多春季消费分析 - 生成带脏数据的源数据
生成三个CSV文件，包含：空值、异常值、格式错误、重复数据、逻辑错误
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# 设置随机种子
np.random.seed(42)
random.seed(42)


# ==================== 1. 生成用户数据（带脏数据） ====================
def generate_users_dirty(n_users=2000):
    """生成用户数据，包含空值、异常值、格式错误"""

    users = []

    for i in range(n_users):
        user_id = i + 1

        # 城市：南昌/深圳，但部分为空（5%）
        if random.random() < 0.05:
            city = None  # 空值
        else:
            city = random.choice(['南昌', '深圳'])

        # 年龄：正常18-65，异常值（年龄>100或<0）占3%
        age_rand = random.random()
        if age_rand < 0.03:
            age = random.choice([-5, 0, 150, 200])  # 异常值
        elif age_rand < 0.08:
            age = None  # 空值
        else:
            age = random.randint(18, 65)

        # 性别：正常，但部分为空
        if random.random() < 0.04:
            gender = None
        else:
            gender = random.choice(['male', 'female'])

        # 会员等级：部分异常值（修复：使用 random.choices）
        level_rand = random.random()
        if level_rand < 0.03:
            membership_level = random.choice(['VIP', 'SVIP', '金卡', ''])  # 异常值
        elif level_rand < 0.07:
            membership_level = None
        else:
            # 修复：random.choices 返回列表，取第一个元素
            membership_level = random.choices(['普通', '黄金', '铂金'], weights=[0.6, 0.3, 0.1])[0]

        # 注册日期：部分格式错误
        if random.random() < 0.04:
            registration_date = random.choice(['2024/13/01', '2025-02-30', '2026.13.01', 'invalid_date'])
        elif random.random() < 0.03:
            registration_date = None
        else:
            days_ago = random.randint(365, 1095)
            registration_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')

        # 新增：手机号（部分空值/格式错误）
        if random.random() < 0.06:
            phone_number = None
        elif random.random() < 0.04:
            phone_number = random.choice(['123456', 'abcdefg', '138001380000', '123-4567-8901'])
        else:
            phone_number = f'1{random.randint(30, 89)}' + ''.join([str(random.randint(0, 9)) for _ in range(9)])

        # 新增：最后登录日期
        if random.random() < 0.05:
            last_login_date = None
        else:
            days_ago = random.randint(1, 60)
            last_login_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')

        # 新增：用户来源
        user_source = random.choice(['自然搜索', '广告', '分享链接', '直接访问', ''])

        # 新增：备注字段（大部分为空）
        remarks = '' if random.random() < 0.8 else f'备注{random.randint(1, 100)}'

        users.append({
            'user_id': user_id,
            'city': city,
            'age': age,
            'gender': gender,
            'membership_level': membership_level,
            'registration_date': registration_date,
            'phone_number': phone_number,
            'last_login_date': last_login_date,
            'user_source': user_source,
            'remarks': remarks
        })

    return pd.DataFrame(users)


# ==================== 2. 生成商品数据（带脏数据） ====================
def generate_products_dirty(n_products=500):
    """生成商品数据，包含空值、异常值、格式错误"""

    categories = {
        '服饰鞋帽': {'pct': 0.35, 'price_range': (30, 150), 'domestic_rate': 0.70},
        '美妆个护': {'pct': 0.20, 'price_range': (25, 120), 'domestic_rate': 0.65},
        '家居百货': {'pct': 0.15, 'price_range': (15, 90), 'domestic_rate': 0.80},
        '食品零食': {'pct': 0.12, 'price_range': (15, 70), 'domestic_rate': 0.90},
        '数码配件': {'pct': 0.08, 'price_range': (40, 200), 'domestic_rate': 0.55},
        '母婴用品': {'pct': 0.05, 'price_range': (30, 160), 'domestic_rate': 0.65},
        '宠物用品': {'pct': 0.05, 'price_range': (20, 100), 'domestic_rate': 0.75}
    }

    products = []
    product_id = 1

    for category, config in categories.items():
        target_count = int(n_products * config['pct'])

        for _ in range(target_count):
            # 价格：异常值（负数、极大值）占3%
            price_rand = random.random()
            if price_rand < 0.02:
                price = -random.uniform(10, 50)  # 负数价格
            elif price_rand < 0.03:
                price = random.uniform(10000, 50000)  # 极高价格
            elif price_rand < 0.06:
                price = None
            else:
                price = round(random.uniform(*config['price_range']), 2)

            # 原价
            if price and price > 0 and not isinstance(price, str):
                original_price = round(price * random.uniform(1.1, 1.4), 2)
            else:
                original_price = None

            # 品牌类型：部分异常
            if random.random() < 0.05:
                brand_type = random.choice(['', 'other', 'unknown', '杂牌'])
            else:
                brand_type = '国货' if random.random() < config['domestic_rate'] else '国际'

            # 销量：异常值（负数）
            if random.random() < 0.02:
                sales_volume = -random.randint(1, 100)
            elif random.random() < 0.05:
                sales_volume = None
            else:
                sales_volume = random.randint(0, 8000)

            # 评分：异常值（超出1-5范围）
            if random.random() < 0.03:
                rating = random.uniform(6, 10)
            elif random.random() < 0.04:
                rating = None
            else:
                rating = round(random.uniform(3.5, 4.9), 1)

            # 新增：供应商（部分为空）
            supplier = '' if random.random() < 0.3 else f'供应商{random.randint(1, 50)}'

            # 新增：仓库城市
            warehouse_city = random.choice(['南昌', '深圳', '广州', '武汉', '', None])

            # 新增：商品描述（含特殊字符）
            if random.random() < 0.1:
                product_description = None
            else:
                special_chars = random.choice(['', '【特惠】', '🔥爆款🔥', '<script>', '   ', '商品 名称 带 空格'])
                product_description = f'{special_chars}{category}商品{product_id}'

            products.append({
                'product_id': product_id,
                'category': category,
                'brand_type': brand_type,
                'price': price,
                'original_price': original_price,
                'sales_volume': sales_volume,
                'rating': rating,
                'supplier': supplier,
                'warehouse_city': warehouse_city,
                'product_description': product_description
            })
            product_id += 1

    # 补足数量
    while len(products) < n_products:
        category = random.choice(list(categories.keys()))
        products.append({
            'product_id': product_id,
            'category': category,
            'brand_type': random.choice(['国货', '国际', '']),
            'price': random.uniform(10, 200),
            'original_price': random.uniform(20, 300),
            'sales_volume': random.randint(0, 5000),
            'rating': round(random.uniform(3, 5), 1),
            'supplier': f'供应商{random.randint(1, 50)}',
            'warehouse_city': random.choice(['南昌', '深圳', '广州']),
            'product_description': f'商品描述{product_id}'
        })
        product_id += 1

    return pd.DataFrame(products)


# ==================== 3. 生成订单数据（带脏数据） ====================
def generate_orders_dirty(users_df, products_df, n_orders=10000):
    """生成订单数据，包含空值、异常值、重复数据、逻辑错误"""

    orders = []
    start_date = datetime(2026, 3, 1)
    end_date = datetime(2026, 5, 31)
    date_range = (end_date - start_date).days

    users_list = users_df.to_dict('records')
    products_list = products_df.to_dict('records')

    for i in range(n_orders):
        # 模拟重复数据：2%的概率重复上一条订单
        if i > 0 and random.random() < 0.02:
            orders.append(orders[-1].copy())
            orders[-1]['order_id'] = i + 1
            continue

        user = random.choice(users_list)
        product = random.choice(products_list)

        # 订单时间：部分异常（超出范围）
        if random.random() < 0.02:
            order_time = datetime(2027, random.randint(1, 12), random.randint(1, 28))
        elif random.random() < 0.02:
            order_time = datetime(2024, random.randint(1, 12), random.randint(1, 28))
        else:
            random_days = random.randint(0, date_range)
            order_date = start_date + timedelta(days=random_days)
            random_hour = random.choices(range(24), weights=[1] * 18 + [4] * 6)[0]
            order_time = datetime(order_date.year, order_date.month, order_date.day, random_hour, random.randint(0, 59))

        # 支付时间：可能为空（未支付）
        if random.random() < 0.08:
            payment_time = None
        elif random.random() < 0.03:
            payment_time = 'invalid_time_format'
        else:
            payment_time = order_time + timedelta(minutes=random.randint(1, 120))

        # 数量：异常值（负数）
        if random.random() < 0.02:
            quantity = -random.randint(1, 5)
        elif random.random() < 0.03:
            quantity = None
        else:
            quantity = random.randint(1, 5)

        # 单价：可能为负
        if product['price'] and isinstance(product['price'], (int, float)) and product['price'] > 0:
            if random.random() < 0.02:
                unit_price = -product['price']
            else:
                unit_price = product['price']
        else:
            unit_price = None

        # 优惠券金额：可能为负数（bug）
        if random.random() < 0.05:
            coupon_discount = -random.uniform(1, 20)
        elif random.random() < 0.05:
            coupon_discount = None
        else:
            coupon_discount = round(random.uniform(0, 20), 2)

        # 总金额
        if quantity and unit_price and quantity > 0 and unit_price > 0:
            total_amount = round(
                unit_price * quantity - (coupon_discount if coupon_discount and coupon_discount > 0 else 0), 2)
            if total_amount < 0:
                total_amount = 0
        else:
            total_amount = None

        # 逻辑错误：拼团订单但分享次数为0（4%）
        is_group_order = random.random() < 0.4
        if is_group_order and random.random() < 0.1:
            share_count = 0  # 逻辑错误
        elif is_group_order:
            share_count = random.randint(1, 5)
        else:
            share_count = random.randint(0, 2)

        # 物流状态：部分为空
        logistics_status = random.choice(['待发货', '已发货', '已签收', '退货中', '', None])

        # 退款状态
        refund_status = random.choice([None, '', '申请中', '已退款', '已拒绝'])

        # 设备类型
        device_type = random.choice(['iOS', 'Android', 'PC', '', None])

        # 用户代理（杂乱文本）
        user_agent = random.choice([
            'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X)',
            'Mozilla/5.0 (Linux; Android 11; SM-G991B)',
            'random gibberish string @#$%',
            'python-requests/2.28.1',
            None
        ])

        orders.append({
            'order_id': i + 1,
            'user_id': user['user_id'] if user['user_id'] else None,
            'product_id': product['product_id'],
            'order_time': order_time,
            'payment_time': payment_time,
            'quantity': quantity,
            'unit_price': unit_price,
            'coupon_discount': coupon_discount,
            'total_amount': total_amount,
            'is_group_order': is_group_order,
            'share_count': share_count,
            'logistics_status': logistics_status,
            'refund_status': refund_status,
            'device_type': device_type,
            'user_agent': user_agent
        })

    return pd.DataFrame(orders)


# ==================== 4. 主函数 ====================
def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(current_dir)
    data_raw_dir = os.path.join(project_dir, 'data', 'raw')

    os.makedirs(data_raw_dir, exist_ok=True)

    print("=" * 60)
    print("生成带脏数据的源数据...")
    print("=" * 60)

    # 生成用户数据
    n_users = random.randint(1950, 2050)
    print(f"\n1. 生成用户数据... (目标: {n_users} 人)")
    users_df = generate_users_dirty(n_users)
    print(f"   实际生成: {len(users_df)} 人")
    print(f"   空值数量: {users_df.isnull().sum().sum()}")

    # 生成商品数据
    n_products = random.randint(480, 520)
    print(f"\n2. 生成商品数据... (目标: {n_products} 件)")
    products_df = generate_products_dirty(n_products)
    print(f"   实际生成: {len(products_df)} 件")
    print(f"   空值数量: {products_df.isnull().sum().sum()}")

    # 生成订单数据
    n_orders = random.randint(9800, 10200)
    print(f"\n3. 生成订单数据... (目标: {n_orders} 条)")
    orders_df = generate_orders_dirty(users_df, products_df, n_orders)
    print(f"   实际生成: {len(orders_df)} 条")
    print(f"   空值数量: {orders_df.isnull().sum().sum()}")

    # 保存
    users_df.to_csv(os.path.join(data_raw_dir, 'users.csv'), index=False, encoding='utf-8-sig')
    products_df.to_csv(os.path.join(data_raw_dir, 'products.csv'), index=False, encoding='utf-8-sig')
    orders_df.to_csv(os.path.join(data_raw_dir, 'orders.csv'), index=False, encoding='utf-8-sig')

    print("\n" + "=" * 60)
    print("数据生成完成！文件保存路径:")
    print(f"   {data_raw_dir}")
    print("=" * 60)

    # 脏数据统计
    print("\n【脏数据统计】")
    print(f"\n用户表空值:\n{users_df.isnull().sum()}")
    print(f"\n商品表空值:\n{products_df.isnull().sum()}")
    print(f"\n订单表空值:\n{orders_df.isnull().sum()}")


if __name__ == '__main__':
    main()