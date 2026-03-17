import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing."""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("loan_pipeline_tests") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()


@pytest.fixture(scope="session")
def sample_df(spark):
    """Minimal sample dataset mirroring the real schema."""
    data = [
        (1, 2, "Graduate", "No", 800000, 500000, 12, 720,
         1000000, 500000, 200000, 300000, "Approved"),
        (2, 0, "Not Graduate", "Yes", 300000, 1500000, 24, 480,
         200000, 100000, 50000, 100000, "Rejected"),
        (3, 1, "Graduate", "No", 600000, 800000, 18, 680,
         800000, 300000, 150000, 250000, "Approved"),
        (4, 3, "Not Graduate", "Yes", 150000, 2000000, 36, 390,
         100000, 50000, 20000, 80000, "Rejected"),
        (5, 1, "Graduate", "No", 1200000, 700000, 6, 780,
         2000000, 1000000, 500000, 600000, "Approved"),
    ]
    columns = [
        "loan_id", "no_of_dependents", "education", "self_employed",
        "income_annum", "loan_amount", "loan_term", "cibil_score",
        "residential_assets_value", "commercial_assets_value",
        "luxury_assets_value", "bank_asset_value", "loan_status",
    ]
    return spark.createDataFrame(data, columns)


# ── Schema tests ──────────────────────────────────────────────────────

def test_schema_has_required_columns(sample_df):
    required = {
        "loan_id", "education", "self_employed", "income_annum",
        "loan_amount", "cibil_score", "loan_status",
    }
    assert required.issubset(set(sample_df.columns))


def test_row_count(sample_df):
    assert sample_df.count() == 5


# ── Cleaning tests ────────────────────────────────────────────────────

def test_string_trimming(sample_df):
    cleaned = sample_df \
        .withColumn("education", F.trim(F.lower(F.col("education")))) \
        .withColumn("loan_status", F.trim(F.lower(F.col("loan_status"))))

    educations = [r.education for r in cleaned.select("education").collect()]
    statuses = [r.loan_status for r in cleaned.select("loan_status").collect()]

    assert all(" " not in e for e in educations)
    assert all(" " not in s for s in statuses)
    assert all(e == e.lower() for e in educations)


def test_no_nulls_after_dropna(sample_df):
    cleaned = sample_df.dropna(subset=[
        "loan_amount", "income_annum", "cibil_score", "loan_status",
    ])
    assert cleaned.count() == sample_df.count()


# ── Feature engineering tests ─────────────────────────────────────────

def test_is_approved_flag(sample_df):
    df = sample_df \
        .withColumn("loan_status", F.trim(F.lower(F.col("loan_status")))) \
        .withColumn(
            "is_approved",
            F.when(F.col("loan_status") == "approved", 1).otherwise(0),
        )
    flags = [r.is_approved for r in df.select("is_approved").collect()]
    assert set(flags).issubset({0, 1})
    assert sum(flags) == 3


def test_debt_to_income_positive(sample_df):
    df = sample_df \
        .withColumn("income_annum", F.col("income_annum").cast(DoubleType())) \
        .withColumn("loan_amount", F.col("loan_amount").cast(DoubleType())) \
        .withColumn(
            "debt_to_income",
            F.round(F.col("loan_amount") / F.col("income_annum"), 4),
        )
    ratios = [r.debt_to_income for r in df.select("debt_to_income").collect()]
    assert all(r > 0 for r in ratios)


def test_total_assets_calculation(sample_df):
    df = sample_df \
        .withColumn(
            "residential_assets_value",
            F.col("residential_assets_value").cast(DoubleType()),
        ) \
        .withColumn(
            "commercial_assets_value",
            F.col("commercial_assets_value").cast(DoubleType()),
        ) \
        .withColumn(
            "luxury_assets_value",
            F.col("luxury_assets_value").cast(DoubleType()),
        ) \
        .withColumn(
            "bank_asset_value",
            F.col("bank_asset_value").cast(DoubleType()),
        ) \
        .withColumn(
            "total_assets",
            F.col("residential_assets_value")
            + F.col("commercial_assets_value")
            + F.col("luxury_assets_value")
            + F.col("bank_asset_value"),
        )
    first = df.select("total_assets").first().total_assets
    assert first == 2000000.0


def test_cibil_band_assignment(sample_df):
    df = sample_df \
        .withColumn("cibil_score", F.col("cibil_score").cast(DoubleType())) \
        .withColumn(
            "cibil_band",
            F.when(F.col("cibil_score") < 500, "Poor (<500)")
             .when(F.col("cibil_score") < 650, "Fair (500-649)")
             .when(F.col("cibil_score") < 750, "Good (650-749)")
             .otherwise("Excellent (750+)"),
        )
    bands = {
        r.cibil_score: r.cibil_band
        for r in df.select("cibil_score", "cibil_band").collect()
    }
    assert bands[720.0] == "Good (650-749)"
    assert bands[480.0] == "Poor (<500)"
    assert bands[780.0] == "Excellent (750+)"
    assert bands[390.0] == "Poor (<500)"


def test_income_tier_assignment(sample_df):
    df = sample_df \
        .withColumn("income_annum", F.col("income_annum").cast(DoubleType())) \
        .withColumn(
            "income_tier",
            F.when(F.col("income_annum") < 3_000_000, "Low")
             .when(F.col("income_annum") < 7_000_000, "Middle")
             .when(F.col("income_annum") < 12_000_000, "Upper-middle")
             .otherwise("High"),
        )
    tiers = {
        r.income_annum: r.income_tier
        for r in df.select("income_annum", "income_tier").collect()
    }
    assert tiers[800000.0] == "Low"
    assert tiers[600000.0] == "Low"
    assert tiers[1200000.0] == "Low"


# ── Aggregation tests ─────────────────────────────────────────────────

def test_agg_approval_by_education_columns(sample_df):
    df = sample_df \
        .withColumn("education", F.trim(F.lower(F.col("education")))) \
        .withColumn("loan_status", F.trim(F.lower(F.col("loan_status")))) \
        .withColumn(
            "is_approved",
            F.when(F.col("loan_status") == "approved", 1).otherwise(0),
        ) \
        .withColumn("loan_amount", F.col("loan_amount").cast(DoubleType())) \
        .withColumn("income_annum", F.col("income_annum").cast(DoubleType())) \
        .withColumn("cibil_score", F.col("cibil_score").cast(DoubleType()))

    agg = df.groupBy("education", "loan_status").agg(
        F.count("*").alias("count"),
        F.round(F.avg("loan_amount"), 2).alias("avg_loan_amount"),
        F.round(F.avg("income_annum"), 2).alias("avg_income"),
        F.round(F.avg("cibil_score"), 2).alias("avg_cibil_score"),
    )
    expected_cols = {
        "education", "loan_status", "count",
        "avg_loan_amount", "avg_income", "avg_cibil_score",
    }
    assert expected_cols.issubset(set(agg.columns))
    assert agg.count() > 0
    