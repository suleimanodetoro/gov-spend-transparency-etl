import transforms as T
import quality as Q
from pyspark.sql.types import StructType, StructField, StringType, MapType

SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("supplier_name", StringType()),
    StructField("payment_date", StringType()),
    StructField("amount", StringType()),
    StructField("expense_category", StringType()),
    StructField("vat_amount", StringType()),
    StructField("raw_extras", MapType(StringType(), StringType())),
    StructField("source_file", StringType()),
    StructField("source_department", StringType()),
    StructField("schema_version", StringType()),
])


def _prep(spark, rows):
    df = spark.createDataFrame(rows, SCHEMA)
    return Q.split_valid_quarantine(T.add_record_hash(T.clean(df)))


def test_negative_amount_is_kept_and_flagged_not_rejected(spark):
    valid, rejected = _prep(spark, [("T1", "ACME", "01/05/2026", "(500.00)", "Cat", None, {}, "f", "mod", "v")])
    assert valid.count() == 1 and rejected.count() == 0
    r = valid.collect()[0]
    assert r["amount_gbp"] == -500.0 and r["needs_review"] is True


def test_missing_supplier_and_bad_amount_are_quarantined(spark):
    valid, rejected = _prep(spark, [
        ("T2", "", "01/05/2026", "100", "Cat", None, {}, "f", "mod", "v"),
        ("T3", "ACME", "01/05/2026", "INVALID", "Cat", None, {}, "f", "mod", "v"),
    ])
    reasons = " ".join(r["rejection_reason"] for r in rejected.collect())
    assert valid.count() == 0
    assert "missing_supplier" in reasons and "invalid_amount" in reasons
