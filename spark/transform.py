import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ── Args ──────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--project",        required=True)
parser.add_argument("--input_dataset",  required=True)
parser.add_argument("--input_table",    required=True)
parser.add_argument("--output_dataset", required=True)
args = parser.parse_args()

BQ_INPUT  = f"{args.project}.{args.input_dataset}.{args.input_table}"
BQ_OUTPUT = f"{args.project}.{args.output_dataset}"
BUCKET    = f"{args.project}-loan-data-lake"

# ── Spark Session ─────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("LoanApprovalTransform") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.conf.set("temporaryGcsBucket", BUCKET)

print("Reading from BigQuery raw layer...")
df = spark.read.format("bigquery").option("table", BQ_INPUT).load()

print(f"Raw row count: {df.count()}")
df.printSchema()

# ── 1. Clean & Cast ───────────────────────────────────────────────────
df = df \
    .withColumn("income_annum",              F.col("income_annum").cast(DoubleType())) \
    .withColumn("loan_amount",               F.col("loan_amount").cast(DoubleType())) \
    .withColumn("loan_term",                 F.col("loan_term").cast(DoubleType())) \
    .withColumn("cibil_score",               F.col("cibil_score").cast(DoubleType())) \
    .withColumn("residential_assets_value",  F.col("residential_assets_value").cast(DoubleType())) \
    .withColumn("commercial_assets_value",   F.col("commercial_assets_value").cast(DoubleType())) \
    .withColumn("luxury_assets_value",       F.col("luxury_assets_value").cast(DoubleType())) \
    .withColumn("bank_asset_value",          F.col("bank_asset_value").cast(DoubleType())) \
    .withColumn("no_of_dependents",          F.col("no_of_dependents").cast(DoubleType()))

# Normalize string columns
df = df \
    .withColumn("education",     F.trim(F.lower(F.col("education")))) \
    .withColumn("self_employed", F.trim(F.lower(F.col("self_employed")))) \
    .withColumn("loan_status",   F.trim(F.lower(F.col("loan_status"))))

# Drop rows with nulls in critical columns
df = df.dropna(subset=[
    "loan_amount", "income_annum", "cibil_score", "loan_status"
])

# ── 2. Feature Engineering ────────────────────────────────────────────

# Binary approval flag
df = df.withColumn(
    "is_approved",
    F.when(F.col("loan_status") == "approved", 1).otherwise(0)
)

# Debt-to-income ratio
df = df.withColumn(
    "debt_to_income",
    F.round(F.col("loan_amount") / F.col("income_annum"), 4)
)

# Total assets
df = df.withColumn(
    "total_assets",
    F.col("residential_assets_value") +
    F.col("commercial_assets_value") +
    F.col("luxury_assets_value") +
    F.col("bank_asset_value")
)

# CIBIL score band (for dashboard tile 2)
df = df.withColumn(
    "cibil_band",
    F.when(F.col("cibil_score") < 500, "Poor (<500)")
     .when(F.col("cibil_score") < 650, "Fair (500-649)")
     .when(F.col("cibil_score") < 750, "Good (650-749)")
     .otherwise("Excellent (750+)")
)

# Loan amount bucket
df = df.withColumn(
    "loan_amount_bucket",
    F.when(F.col("loan_amount") < 5_000_000,  "< 5M")
     .when(F.col("loan_amount") < 10_000_000, "5M - 10M")
     .when(F.col("loan_amount") < 20_000_000, "10M - 20M")
     .otherwise("> 20M")
)

# Income tier
df = df.withColumn(
    "income_tier",
    F.when(F.col("income_annum") < 3_000_000,  "Low")
     .when(F.col("income_annum") < 7_000_000,  "Middle")
     .when(F.col("income_annum") < 12_000_000, "Upper-middle")
     .otherwise("High")
)

print("Enriched schema:")
df.printSchema()

# ── 3. Mart Table 1: fact_loans (partitioned + clustered) ────────────
print("Writing fact_loans...")

# Write to a temp table first, then use BigQuery DDL to recreate
# with partitioning and clustering
df.write \
    .format("bigquery") \
    .option("table", f"{BQ_OUTPUT}.fact_loans_temp") \
    .option("temporaryGcsBucket", BUCKET) \
    .mode("overwrite") \
    .save()
# ── 4. Mart Table 2: agg_approval_by_education ───────────────────────
agg_education = df.groupBy("education", "loan_status").agg(
    F.count("*").alias("count"),
    F.round(F.avg("loan_amount"), 2).alias("avg_loan_amount"),
    F.round(F.avg("income_annum"), 2).alias("avg_income"),
    F.round(F.avg("cibil_score"), 2).alias("avg_cibil_score")
)

print("Writing agg_approval_by_education...")
agg_education.write \
    .format("bigquery") \
    .option("table", f"{BQ_OUTPUT}.agg_approval_by_education") \
    .option("temporaryGcsBucket", BUCKET) \
    .mode("overwrite") \
    .save()

# ── 5. Mart Table 3: agg_approval_by_cibil_band ──────────────────────
agg_cibil = df.groupBy("cibil_band", "loan_status").agg(
    F.count("*").alias("count"),
    F.round(F.avg("loan_amount"), 2).alias("avg_loan_amount"),
    F.round(F.sum("is_approved"), 2).alias("approved_count"),
    F.round(
        F.sum("is_approved") / F.count("*") * 100, 2
    ).alias("approval_rate_pct")
)

print("Writing agg_approval_by_cibil_band...")
agg_cibil.write \
    .format("bigquery") \
    .option("table", f"{BQ_OUTPUT}.agg_approval_by_cibil_band") \
    .option("temporaryGcsBucket", BUCKET) \
    .mode("overwrite") \
    .save()

# ── 6. Mart Table 4: agg_approval_by_income_tier ─────────────────────
agg_income = df.groupBy("income_tier", "loan_status", "education").agg(
    F.count("*").alias("count"),
    F.round(F.avg("loan_amount"), 2).alias("avg_loan_amount"),
    F.round(F.avg("debt_to_income"), 4).alias("avg_debt_to_income"),
    F.round(
        F.sum("is_approved") / F.count("*") * 100, 2
    ).alias("approval_rate_pct")
)

print("Writing agg_approval_by_income_tier...")
agg_income.write \
    .format("bigquery") \
    .option("table", f"{BQ_OUTPUT}.agg_approval_by_income_tier") \
    .option("temporaryGcsBucket", BUCKET) \
    .mode("overwrite") \
    .save()

print("All mart tables written successfully.")
spark.stop()