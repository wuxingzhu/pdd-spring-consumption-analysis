import pandas as pd
from sqlalchemy import create_engine

# 密码处理
password = "你的MYSQL密码"
password_encoded = password.replace('@', '%40')

# 创建数据库连接
engine = create_engine(f'mysql+pymysql://root:{password_encoded}@localhost:3306/pdd_analysis?charset=utf8mb4')

# 读取 CSV 文件
users = pd.read_csv('D:/users_clean.csv', encoding='utf-8-sig')
products = pd.read_csv('D:/products_clean.csv', encoding='utf-8-sig')
orders = pd.read_csv('D:/orders_clean.csv', encoding='utf-8-sig')

# 导入到 MySQL
users.to_sql('users', engine, if_exists='replace', index=False)
products.to_sql('products', engine, if_exists='replace', index=False)
orders.to_sql('orders', engine, if_exists='replace', index=False)

print("导入完成！")
print(f"用户表：{len(users)} 行")
print(f"商品表：{len(products)} 行")
print(f"订单表：{len(orders)} 行")