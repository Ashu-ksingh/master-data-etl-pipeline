"""
Master Data ETL & Validation Pipeline
--------------------------------------
Simulates an enterprise master data migration: extracts raw Customer Master,
Material Master, and Orders (transactional) data, applies validation and
cleaning rules, and loads the results into Hive tables for downstream
reporting.

All cleaning/validation rules below were first verified against the real
source data using pandas (see /verification/verify_logic*.py) before being
ported here. Run this with:

    spark-submit scripts/etl_pipeline.py

Requires: pyspark (`pip install pyspark`)
No cluster or Docker needed — enableHiveSupport() spins up a local embedded
Hive metastore automatically.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ---------------------------------------------------------------------------
# 0. SPARK SESSION (local, with embedded Hive metastore)
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("MasterDataETLPipeline")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", "spark-warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
# Allow multi-format date parsing to return null on mismatch instead of throwing
# (Spark 3.x's newer parser is strict; LEGACY restores lenient pre-3.0 behavior)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

RAW_DIR = "data"
spark.sql("CREATE DATABASE IF NOT EXISTS master_data")
spark.sql("USE master_data")

# ---------------------------------------------------------------------------
# 1. EXTRACT
# ---------------------------------------------------------------------------
customer_raw = spark.read.csv(f"{RAW_DIR}/customer_master_raw.csv", header=True, inferSchema=False)
product_raw = spark.read.csv(f"{RAW_DIR}/material_master_raw.csv", header=True, inferSchema=False)
orders_raw = spark.read.csv(f"{RAW_DIR}/orders_raw.csv", header=True, inferSchema=False)
# NOTE: inferSchema is disabled on purpose. On dirty data, Spark's schema inference
# will decide a column is numeric and silently convert malformed values (e.g. the
# comma-formatted "1,200" in order_amount and price) to null AT READ TIME - before
# any of our cleaning logic runs, permanently losing the recoverable original text.
# Reading everything as strings and casting explicitly avoids this class of bug.

print(f"Raw counts -> customers: {customer_raw.count()}, products: {product_raw.count()}, orders: {orders_raw.count()}")

# ---------------------------------------------------------------------------
# 2. TRANSFORM + VALIDATE: CUSTOMER MASTER
# ---------------------------------------------------------------------------
# Verified issues: 1,800 duplicate customer_ids, 1,040 missing emails,
# inconsistent name casing, mixed phone formats, multi-value device_id field.

customer_clean = (
    customer_raw
    .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
    .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
    .withColumn("email_missing", F.col("email").isNull())
    # strip extension after 'x', then strip all non-digits, keep last 10 digits
    .withColumn("phone_digits", F.regexp_replace(F.split(F.col("phone_number"), "x").getItem(0), r"\D", ""))
    .withColumn("phone_clean", F.expr("substring(phone_digits, greatest(length(phone_digits)-9,1), 10)"))
    .withColumn("device_list", F.split(F.col("device_id(s)"), ";"))
    .withColumn("device_count", F.size(F.col("device_list")))
    .withColumn("primary_device_id", F.col("device_list").getItem(0))
)

# Track duplicate count before dedup (validation metric), then dedup keeping first occurrence
dup_customer_count = customer_clean.count() - customer_clean.dropDuplicates(["customer_id"]).count()
customer_clean = customer_clean.dropDuplicates(["customer_id"])

print(f"Customer Master -> duplicates removed: {dup_customer_count}, "
      f"missing emails flagged: {customer_clean.filter('email_missing').count()}, "
      f"rows after cleaning: {customer_clean.count()}")

# ---------------------------------------------------------------------------
# 3. TRANSFORM + VALIDATE: MATERIAL MASTER
# ---------------------------------------------------------------------------
# Verified issues: 23+ category typos (leetspeak/casing/punctuation), price
# format chaos (commas, whitespace, negative sentinel values, true nulls).

CANONICAL_CATEGORIES = ["clothing", "automotive", "electronics", "toys", "home", "sports", "beauty", "kitchen"]

def normalize_category(raw_col):
    # lowercase, strip junk chars, fix leetspeak (3->e,1->i,0->o,4->a,5->s), strip non-letters
    s = F.lower(F.trim(raw_col))
    s = F.regexp_replace(s, r"^[_\-\s]+|[_\-\s]+$", "")
    s = F.translate(s, "30145", "eoias")
    s = F.regexp_replace(s, r"[^a-z]", "")
    expr = F.lit(None).cast("string")
    for canon in CANONICAL_CATEGORIES:
        expr = F.when((s == canon) | ((F.length(s) >= 3) & (F.lit(canon).startswith(s))), F.lit(canon)).otherwise(expr)
    return expr

product_clean = (
    product_raw
    .withColumn("category_raw", F.col("category"))
    .withColumn("category", normalize_category(F.col("category")))
    .withColumn("category_missing", F.col("category").isNull())
    # price: treat null, empty/whitespace, AND literal missing-value tokens (NaN, null, na, none, n/a)
    # as missing before casting - Spark's cast("double") parses the text "NaN" as a real IEEE NaN,
    # not SQL null, so it must be caught explicitly or it silently passes both null and negative checks.
    .withColumn("price_norm", F.lower(F.trim(F.col("price").cast("string"))))
    .withColumn("price_str",
        F.when(
            F.col("price").isNull() |
            (F.col("price_norm") == "") |
            F.col("price_norm").isin("nan", "null", "na", "n/a", "none", "#n/a"),
            F.lit(None).cast("string")
        ).otherwise(F.regexp_replace(F.trim(F.col("price").cast("string")), ",", "")))
    .withColumn("price_clean", F.col("price_str").cast(DoubleType()))
    .withColumn("price_invalid",
        F.col("price_clean").isNull() | F.isnan(F.col("price_clean")) | (F.col("price_clean") < 0))
    .drop("price_norm")
)

print(f"Material Master -> category unmapped: {product_clean.filter('category_missing').count()}, "
      f"invalid/missing prices: {product_clean.filter('price_invalid').count()}")

# ---------------------------------------------------------------------------
# 4. TRANSFORM + VALIDATE: ORDERS (TRANSACTIONAL)
# ---------------------------------------------------------------------------
# Verified issues: payment_method (11 variants of 4 values), status (10
# variants of 3 values), date chaos including a sentinel placeholder
# "31-12-2023" used 18,050 times to mark bad/missing dates.

SENTINEL_DATE = "31-12-2023"

def normalize_payment(raw_col):
    s = F.regexp_replace(F.lower(F.trim(raw_col)), r"[^a-z]", "")
    return (
        F.when(s.isin("card", "crd", "crad", "cd"), "card")
         .when(s.contains("wall"), "wallet")
         .when(s == "upi", "upi")
         .when(s == "cash", "cash")
         .otherwise(F.lit(None).cast("string"))
    )

def normalize_status(raw_col):
    s = F.lower(F.trim(raw_col))
    return (
        F.when(s.isin("success", "suc"), "success")
         .when(s.isin("refunded", "ref"), "refunded")
         .when(s.isin("failed", "fail"), "failed")
         .otherwise(F.lit(None).cast("string"))
    )

orders_clean = (
    orders_raw
    .withColumn("payment_method_clean", normalize_payment(F.col("payment_method")))
    .withColumn("status_clean", normalize_status(F.col("status")))
    .withColumn("order_date_flag",
        F.when(F.col("order_date").isNull(), "missing")
         .when(F.col("order_date") == SENTINEL_DATE, "sentinel_placeholder")
         .otherwise("ok"))
    .withColumn("order_date_clean",
        F.when(F.col("order_date_flag") == "ok",
               F.coalesce(
                   F.to_date("order_date", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
                   F.to_date("order_date", "yyyy-MM-dd"),
                   F.to_date("order_date", "yyyy/dd/MM"),
                   F.to_date("order_date", "yyyy/MM/dd HH:mm"),
               )).otherwise(F.lit(None).cast("date")))
    # order_amount: Spark's CSV reader infers this as numeric and parses the literal
    # text "NaN" directly into a real floating-point NaN at READ TIME (not SQL null),
    # so isNull() alone misses it - same underlying issue as the price field above.
    # order_amount: same class of issue as price - literal "NaN" text tokens AND
    # comma-formatted values (e.g. "1,200"). Since we now read as StringType, no
    # data is lost at read time; clean it the same way as price above.
    .withColumn("order_amount_norm", F.lower(F.trim(F.col("order_amount"))))
    .withColumn("order_amount_str",
        F.when(
            F.col("order_amount").isNull() |
            (F.col("order_amount_norm") == "") |
            F.col("order_amount_norm").isin("nan", "null", "na", "n/a", "none", "#n/a"),
            F.lit(None).cast("string")
        ).otherwise(F.regexp_replace(F.trim(F.col("order_amount")), ",", "")))
    .withColumn("order_amount_clean", F.col("order_amount_str").cast(DoubleType()))
    .withColumn("order_amount_missing",
        F.col("order_amount_clean").isNull() | F.isnan(F.col("order_amount_clean")))
    .drop("order_amount_norm", "order_amount_str")
)

print(f"Orders -> payment unmapped: {orders_clean.filter(F.col('payment_method_clean').isNull()).count()}, "
      f"status unmapped: {orders_clean.filter(F.col('status_clean').isNull()).count()}, "
      f"date flags: sentinel={orders_clean.filter(F.col('order_date_flag')=='sentinel_placeholder').count()}, "
      f"missing={orders_clean.filter(F.col('order_date_flag')=='missing').count()}")

# ---------------------------------------------------------------------------
# 5. REFERENTIAL INTEGRITY CHECK (orders vs. master tables)
# ---------------------------------------------------------------------------
orphaned_customer_orders = orders_clean.join(
    customer_clean.select("customer_id"), on="customer_id", how="left_anti"
).count()
orphaned_product_orders = orders_clean.join(
    product_clean.select("product_id"), on="product_id", how="left_anti"
).count()
print(f"Referential integrity -> orphaned customer refs: {orphaned_customer_orders}, "
      f"orphaned product refs: {orphaned_product_orders}")

# ---------------------------------------------------------------------------
# 6. LOAD: WRITE TO HIVE TABLES
# ---------------------------------------------------------------------------
customer_clean.write.mode("overwrite").saveAsTable("master_data.customer_master")
product_clean.write.mode("overwrite").saveAsTable("master_data.material_master")
orders_clean.write.mode("overwrite").saveAsTable("master_data.orders_fact")

print("\nLoad complete. Tables available in Hive database 'master_data':")
spark.sql("SHOW TABLES IN master_data").show()

spark.stop()
