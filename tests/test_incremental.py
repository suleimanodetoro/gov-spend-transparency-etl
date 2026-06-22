import pipeline as P
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("record_hash", StringType()),
    StructField("source_department", StringType()),
    StructField("payment_month", StringType()),
    StructField("amount_gbp", DoubleType()),
    StructField("ingested_at", TimestampType()),
])


def _row(tid, amount, hour):
    # same partition (cabinet_office / 2026-04); hour controls ingested_at ordering
    return (tid, f"h-{tid}-{amount}", "cabinet_office", "2026-04", amount, datetime(2026, 6, 1, hour))


def test_incremental_upsert_preserves_existing_and_dedups_corrections(spark, tmp_path):
    silver = str(tmp_path / "silver")
    # first load: T1, T2 already written to the partition
    spark.createDataFrame([_row("T1", 100.0, 9), _row("T2", 200.0, 9)], SCHEMA) \
        .write.mode("overwrite").partitionBy("source_department", "payment_month").parquet(silver)

    # a late file for the SAME partition: T2 corrected (later ingested_at) and T3 new
    new = spark.createDataFrame([_row("T2", 250.0, 10), _row("T3", 300.0, 10)], SCHEMA)
    P.upsert_silver(spark, silver, new)

    out = spark.read.parquet(silver)
    by_id = {r["transaction_id"]: r["amount_gbp"] for r in out.collect()}
    assert out.count() == 3                 # T1 preserved, T2 updated, T3 added, no duplicates
    assert set(by_id) == {"T1", "T2", "T3"}
    assert by_id["T1"] == 100.0             # earlier partition rows were not lost
    assert by_id["T2"] == 250.0             # the correction won (latest ingested_at)
