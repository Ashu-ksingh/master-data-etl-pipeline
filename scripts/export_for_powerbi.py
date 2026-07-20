"""
Export Validation Query Results to CSV (for Power BI)
--------------------------------------------------------
Re-runs the key validation/reconciliation queries against the Hive tables
produced by etl_pipeline.py, and writes each result as a single clean CSV
file into output/ for Power BI to consume.

Run with:
    spark-submit scripts/export_for_powerbi.py

Requires etl_pipeline.py to have already been run at least once (so the
Hive tables exist).
"""

from pyspark.sql import SparkSession
import os

spark = (
    SparkSession.builder
    .appName("ExportValidationResultsForPowerBI")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", "spark-warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
spark.sql("USE master_data")


def export(df, name):
    """Write a small result DataFrame out as a single clean CSV file."""
    path = f"output/{name}_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)
    # Spark writes a part-*.csv file inside a folder; move/rename it to a flat file
    import glob, shutil, os
    part_file = glob.glob(f"{path}/part-*.csv")[0]
    shutil.move(part_file, f"output/{name}.csv")
    shutil.rmtree(path)
    print(f"Wrote output/{name}.csv")


os.makedirs("output", exist_ok=True)

# 1. Customer Master quality summary
customer_summary = spark.sql("""
    SELECT
        COUNT(*) AS total_customers,
        SUM(CASE WHEN email_missing THEN 1 ELSE 0 END) AS missing_email_count,
        ROUND(100.0 * SUM(CASE WHEN email_missing THEN 1 ELSE 0 END) / COUNT(*), 2) AS missing_email_pct,
        SUM(CASE WHEN device_count > 1 THEN 1 ELSE 0 END) AS multi_device_customers,
        ROUND(AVG(device_count), 2) AS avg_devices_per_customer
    FROM customer_master
""")
export(customer_summary, "customer_quality_summary")

# 2. Material Master quality by category
material_by_category = spark.sql("""
    SELECT
        COALESCE(category, '(unmapped)') AS category,
        COUNT(*) AS product_count,
        SUM(CASE WHEN price_invalid THEN 1 ELSE 0 END) AS invalid_price_count,
        ROUND(AVG(price_clean), 2) AS avg_valid_price
    FROM material_master
    GROUP BY COALESCE(category, '(unmapped)')
    ORDER BY product_count DESC
""")
export(material_by_category, "material_quality_by_category")

# 3. Orders date quality breakdown
orders_date_quality = spark.sql("""
    SELECT
        order_date_flag,
        COUNT(*) AS order_count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
    FROM orders_fact
    GROUP BY order_date_flag
""")
export(orders_date_quality, "orders_date_quality")

# 4. Payment method distribution
payment_dist = spark.sql("""
    SELECT payment_method_clean, COUNT(*) AS order_count
    FROM orders_fact
    GROUP BY payment_method_clean
    ORDER BY order_count DESC
""")
export(payment_dist, "orders_payment_distribution")

# 5. Order status distribution
status_dist = spark.sql("""
    SELECT status_clean, COUNT(*) AS order_count
    FROM orders_fact
    GROUP BY status_clean
    ORDER BY order_count DESC
""")
export(status_dist, "orders_status_distribution")

# 6. Revenue by category (cross-reference orders x material master)
revenue_by_category = spark.sql("""
    SELECT
        m.category,
        COUNT(o.order_id) AS order_count,
        ROUND(SUM(o.order_amount_clean), 2) AS total_revenue,
        ROUND(AVG(o.order_amount_clean), 2) AS avg_order_value
    FROM orders_fact o
    JOIN material_master m ON o.product_id = m.product_id
    WHERE o.order_amount_clean IS NOT NULL
    GROUP BY m.category
    ORDER BY total_revenue DESC
""")
export(revenue_by_category, "revenue_by_category")

# 7. Payment method x status cross-tab
payment_status_crosstab = spark.sql("""
    SELECT payment_method_clean, status_clean, COUNT(*) AS order_count
    FROM orders_fact
    GROUP BY payment_method_clean, status_clean
    ORDER BY payment_method_clean, status_clean
""")
export(payment_status_crosstab, "payment_status_crosstab")

# 8. Before/after reconciliation summary (the headline "quality report" table)
reconciliation_summary = spark.sql("""
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
    SELECT 'orders_fact', 'missing_order_amount', SUM(CASE WHEN order_amount_missing THEN 1 ELSE 0 END) FROM orders_fact
""")
export(reconciliation_summary, "reconciliation_summary")

print("\nAll exports complete. CSV files are in the output/ folder, ready for Power BI.")
spark.stop()
