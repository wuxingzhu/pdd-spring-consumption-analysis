-- =====================================================
-- 拼多多春季消费分析 - 核心SQL查询
-- 数据库：pdd_analysis
-- 时间范围：2026-03-01 至 2026-05-31
-- =====================================================

-- 0. 创建有效订单视图（过滤时间范围 + 排除未知城市）
CREATE OR REPLACE VIEW v_valid_orders AS
SELECT 
    o.*,
    u.city,
    u.gender,
    u.age,
    u.membership_level,
    p.category,
    p.brand_type,
    p.price as product_price
FROM orders o
JOIN users u ON o.user_id = u.user_id
JOIN products p ON o.product_id = p.product_id
WHERE o.order_time BETWEEN '2026-03-01' AND '2026-05-31'
  AND u.city IN ('南昌', '深圳');

-- =====================================================
-- Q1: 双城消费画像对比
-- =====================================================
SELECT 
    city,
    COUNT(DISTINCT user_id) AS user_count,
    COUNT(*) AS order_count,
    ROUND(SUM(total_amount), 2) AS total_spent,
    ROUND(AVG(total_amount), 2) AS avg_order_value,
    ROUND(SUM(total_amount) / COUNT(DISTINCT user_id), 2) AS avg_spent_per_user
FROM v_valid_orders
GROUP BY city;

-- =====================================================
-- Q2: 品类偏好对比（TOP5）
-- =====================================================
WITH category_rank AS (
    SELECT 
        city,
        category,
        ROUND(SUM(total_amount), 2) AS category_spent,
        ROUND(SUM(total_amount) / SUM(SUM(total_amount)) OVER (PARTITION BY city) * 100, 2) AS pct_of_total,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY SUM(total_amount) DESC) AS rn
    FROM v_valid_orders
    GROUP BY city, category
)
SELECT city, category, category_spent, pct_of_total
FROM category_rank
WHERE rn <= 5
ORDER BY city, rn;

-- =====================================================
-- Q3: 价格带分析
-- =====================================================
SELECT 
    city,
    CASE 
        WHEN product_price < 50 THEN '0-50元'
        WHEN product_price < 100 THEN '50-100元'
        ELSE '100元以上'
    END AS price_range,
    COUNT(*) AS order_count,
    ROUND(SUM(total_amount), 2) AS total_spent
FROM v_valid_orders
GROUP BY city, price_range
ORDER BY city, price_range;

-- =====================================================
-- Q4: 促销敏感度（使用优惠券 vs 未使用）
-- =====================================================
SELECT 
    city,
    CASE 
        WHEN coupon_discount > 0 THEN '使用优惠券'
        ELSE '未使用优惠券'
    END AS is_promotion,
    COUNT(*) AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY city), 2) AS pct_of_orders,
    ROUND(AVG(coupon_discount), 2) AS avg_discount
FROM v_valid_orders
GROUP BY city, is_promotion;

-- =====================================================
-- Q5: 社交裂变分析
-- =====================================================
SELECT 
    city,
    is_group_order,
    COUNT(*) AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY city), 2) AS pct_of_orders,
    ROUND(AVG(share_count), 2) AS avg_shares
FROM v_valid_orders
GROUP BY city, is_group_order;

-- =====================================================
-- Q6: 时间趋势（周度）
-- =====================================================
SELECT 
    city,
    DATE_FORMAT(order_time, '%Y-%m') AS month,
    WEEK(order_time) AS week_num,
    ROUND(SUM(total_amount), 2) AS weekly_spent
FROM v_valid_orders
GROUP BY city, month, week_num
ORDER BY city, week_num;

-- =====================================================
-- Q7: 用户分层（NTILE版本，兼容MySQL 5.7）
-- =====================================================
WITH user_value AS (
    SELECT 
        city,
        user_id,
        SUM(total_amount) AS user_total,
        NTILE(4) OVER (PARTITION BY city ORDER BY SUM(total_amount)) AS quartile
    FROM v_valid_orders
    GROUP BY city, user_id
)
SELECT 
    city,
    CASE 
        WHEN quartile = 4 THEN '高价值'
        WHEN quartile = 3 THEN '中等偏高'
        WHEN quartile = 2 THEN '中等偏低'
        ELSE '低价值'
    END AS user_tier,
    COUNT(*) AS user_count,
    ROUND(SUM(user_total), 2) AS total_contribution,
    ROUND(AVG(user_total), 2) AS avg_per_user
FROM user_value
GROUP BY city, user_tier
ORDER BY city, quartile DESC;

-- =====================================================
-- Q8: 关联购买（同一天内）
-- =====================================================
SELECT 
    o1.category AS category_1,
    o2.category AS category_2,
    COUNT(*) AS co_occurrence
FROM v_valid_orders o1
JOIN v_valid_orders o2 
    ON o1.user_id = o2.user_id 
    AND DATE(o1.order_time) = DATE(o2.order_time)
    AND o1.category < o2.category
GROUP BY category_1, category_2
ORDER BY co_occurrence DESC
LIMIT 10;