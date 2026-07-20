-- ============================================================================
-- Master Data ETL & Validation Pipeline
-- Data Quality Validation & Reconciliation Queries (HiveQL)
-- ----------------------------------------------------------------------------
-- Run these against the Hive tables produced by scripts/etl_pipeline.py:
--   master_data.customer_master
--   master_data.material_master
--   master_data.orders_fact
--
-- Usage (from a Spark shell / spark-sql with Hive support enabled):
--   spark-sql --conf spark.sql.warehouse.dir=spark-warehouse -f scripts/validation_queries.sql
-- or run each block interactively via spark.sql("...").show() in a notebook.
-- ============================================================================

USE master_data;

-- ----------------------------------------------------------------------------
-- 1. CUSTOMER MASTER — DATA QUALITY SUMMARY
-- ----------------------------------------------------------------------------
SELECT
    COUNT(*)                                   AS total_customers,
    SUM(CASE WHEN email_missing THEN 1 ELSE 0 END)      AS missing_email_count,
    ROUND(100.0 * SUM(CASE WHEN email_missing THEN 1 ELSE 0 END) / COUNT(*), 2) AS missing_email_pct,
    SUM(CASE WHEN device_count > 1 THEN 1 ELSE 0 END)   AS multi_device_customers,
    ROUND(AVG(device_count), 2)                         AS avg_devices_per_customer
FROM customer_master;

-- ----------------------------------------------------------------------------
-- 2. MATERIAL MASTER — DATA QUALITY SUMMARY (by category)
-- ----------------------------------------------------------------------------
SELECT
    COALESCE(category, '(unmapped)')           AS category,
    COUNT(*)                                   AS product_count,
    SUM(CASE WHEN price_invalid THEN 1 ELSE 0 END)      AS invalid_price_count,
    ROUND(AVG(price_clean), 2)                          AS avg_valid_price
FROM material_master
GROUP BY COALESCE(category, '(unmapped)')
ORDER BY product_count DESC;

-- Overall material master quality rate
SELECT
    COUNT(*)                                                       AS total_products,
    SUM(CASE WHEN category_missing THEN 1 ELSE 0 END)              AS category_unmapped_count,
    SUM(CASE WHEN price_invalid THEN 1 ELSE 0 END)                 AS price_invalid_count,
    ROUND(100.0 * (COUNT(*) -
        SUM(CASE WHEN category_missing OR price_invalid THEN 1 ELSE 0 END)) / COUNT(*), 2) AS clean_record_pct
FROM material_master;

-- ----------------------------------------------------------------------------
-- 3. ORDERS — DATA QUALITY SUMMARY
-- ----------------------------------------------------------------------------
SELECT
    order_date_flag,
    COUNT(*)                                   AS order_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM orders_fact
GROUP BY order_date_flag
ORDER BY order_count DESC;

SELECT
    payment_method_clean,
    COUNT(*)                                   AS order_count
FROM orders_fact
GROUP BY payment_method_clean
ORDER BY order_count DESC;

SELECT
    status_clean,
    COUNT(*)                                   AS order_count
FROM orders_fact
GROUP BY status_clean
ORDER BY order_count DESC;

-- Missing order_amount rate (NaN-token rows now correctly counted via order_amount_missing)
SELECT
    COUNT(*)                                                   AS total_orders,
    SUM(CASE WHEN order_amount_missing THEN 1 ELSE 0 END)      AS missing_amount_count,
    ROUND(100.0 * SUM(CASE WHEN order_amount_missing THEN 1 ELSE 0 END) / COUNT(*), 2) AS missing_amount_pct
FROM orders_fact;

-- ----------------------------------------------------------------------------
-- 4. REFERENTIAL INTEGRITY RECONCILIATION
-- (orders should always reference an existing customer and product)
-- ----------------------------------------------------------------------------
SELECT
    'orphaned_customer_refs' AS check_name,
    COUNT(*) AS violation_count
FROM orders_fact o
LEFT ANTI JOIN customer_master c ON o.customer_id = c.customer_id

UNION ALL

SELECT
    'orphaned_product_refs' AS check_name,
    COUNT(*) AS violation_count
FROM orders_fact o
LEFT ANTI JOIN material_master m ON o.product_id = m.product_id;

-- ----------------------------------------------------------------------------
-- 5. CROSS-REFERENCE: ORDERS vs. MASTER DATA (mirrors Wipro-style
--    inventory/sales cross-referencing work)
-- ----------------------------------------------------------------------------
-- Revenue by product category, joining orders to material master
-- (uses order_amount_clean, which has NaN-token rows properly nulled out -
--  the raw order_amount column would silently poison SUM/AVG with NaN otherwise)
SELECT
    m.category,
    COUNT(o.order_id)                          AS order_count,
    ROUND(SUM(o.order_amount_clean), 2)         AS total_revenue,
    ROUND(AVG(o.order_amount_clean), 2)         AS avg_order_value
FROM orders_fact o
JOIN material_master m ON o.product_id = m.product_id
WHERE o.order_amount_clean IS NOT NULL
GROUP BY m.category
ORDER BY total_revenue DESC;

-- Order success/failure rate by payment method (data quality x business insight)
SELECT
    payment_method_clean,
    status_clean,
    COUNT(*) AS order_count
FROM orders_fact
GROUP BY payment_method_clean, status_clean
ORDER BY payment_method_clean, status_clean;

-- ----------------------------------------------------------------------------
-- 6. BEFORE vs AFTER RECONCILIATION SUMMARY
-- (single table summarizing the cleaning impact - feeds the Power BI report)
-- ----------------------------------------------------------------------------
SELECT 'customer_master' AS table_name, 'duplicate_ids_removed' AS issue_type, 1800 AS record_count
UNION ALL
SELECT 'customer_master', 'missing_email', SUM(CASE WHEN email_missing THEN 1 ELSE 0 END) FROM customer_master
UNION ALL
SELECT 'material_master', 'category_unmapped', SUM(CASE WHEN category_missing THEN 1 ELSE 0 END) FROM material_master
UNION ALL
SELECT 'material_master', 'invalid_price', SUM(CASE WHEN price_invalid THEN 1 ELSE 0 END) FROM material_master
UNION ALL
SELECT 'orders_fact', 'sentinel_placeholder_date', SUM(CASE WHEN order_date_flag = 'sentinel_placeholder' THEN 1 ELSE 0 END) FROM orders_fact
UNION ALL
SELECT 'orders_fact', 'missing_date', SUM(CASE WHEN order_date_flag = 'missing' THEN 1 ELSE 0 END) FROM orders_fact
UNION ALL
SELECT 'orders_fact', 'missing_order_amount', SUM(CASE WHEN order_amount_missing THEN 1 ELSE 0 END) FROM orders_fact;
