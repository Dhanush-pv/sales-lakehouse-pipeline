import pytest

from sales_lakehouse_pipeline import quality

COLUMNS = ["customer_id", "amount", "region", "updated_at"]
SCHEMA = "customer_id int, amount double, region string, updated_at string"


@pytest.fixture()
def good_df(spark):
    return spark.createDataFrame(
        [(1, 100.0, "EU", "2026-01-01"), (2, 200.0, "US", "2026-01-02")], SCHEMA
    )


def test_row_count_passes_and_fails(spark, good_df):
    assert quality.check_row_count(good_df, "t", 2).passed
    assert not quality.check_row_count(good_df, "t", 3).passed
    assert not quality.check_row_count(spark.createDataFrame([], SCHEMA), "t").passed


def test_nulls_detected(spark, good_df):
    assert quality.check_no_nulls(good_df, "t", ["customer_id", "amount"]).passed
    with_null = spark.createDataFrame(
        [(1, None, "EU", "2026-01-01"), (None, 5.0, "US", "2026-01-02")], SCHEMA
    )
    result = quality.check_no_nulls(with_null, "t", ["customer_id", "amount"])
    assert not result.passed
    assert "customer_id" in result.detail and "amount" in result.detail


def test_duplicate_keys_detected(spark, good_df):
    assert quality.check_unique_key(good_df, "t", ["customer_id"]).passed
    dup = spark.createDataFrame(
        [(1, 100.0, "EU", "2026-01-01"), (1, 150.0, "EU", "2026-01-03")], SCHEMA
    )
    result = quality.check_unique_key(dup, "t", ["customer_id"])
    assert not result.passed
    assert "1 duplicated keys" in result.detail


def test_row_count_not_below_reference(good_df):
    assert quality.check_row_count_not_below(good_df, "silver", 2, "bronze keys").passed
    assert not quality.check_row_count_not_below(good_df, "silver", 3, "bronze keys").passed


def test_enforce_raises_when_any_check_fails(good_df):
    ok = quality.check_row_count(good_df, "t", 1)
    bad = quality.check_row_count(good_df, "t", 99)
    quality.enforce([ok])  # does not raise
    with pytest.raises(quality.DataQualityError):
        quality.enforce([ok, bad])
