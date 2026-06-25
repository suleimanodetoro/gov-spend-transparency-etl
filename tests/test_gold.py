import pipeline as P
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

SCHEMA = StructType([
    StructField("source_department", StringType()),
    StructField("department_group", StringType()),
    StructField("payment_month", StringType()),
    StructField("supplier_name", StringType()),
    StructField("amount_gbp", DoubleType()),
    StructField("needs_review", BooleanType()),
])


def test_aggregate_gold_nets_credits_counts_distinct_suppliers_and_credits(spark):
    # One department-month: two normal payments to the same supplier + one credit to another.
    rows = [
        ("mod", "Defence", "2026-04", "Acme", 100.0, False),
        ("mod", "Defence", "2026-04", "Acme", 200.0, False),
        ("mod", "Defence", "2026-04", "Beta", -50.0, True),   # credit (needs_review)
    ]
    g = P.aggregate_gold(spark.createDataFrame(rows, SCHEMA)).collect()
    assert len(g) == 1
    r = g[0]
    assert str(r["total_spend"]) == "250.00"      # 100 + 200 - 50: credits NET, not dropped
    assert r["transaction_count"] == 3
    assert r["supplier_count"] == 2               # Acme counted once (countDistinct)
    assert r["credit_count"] == 1                 # exactly the needs_review row
    assert str(r["avg_amount"]) == "83.33"        # 250/3 -> decimal(18,2)


def test_aggregate_gold_splits_by_department_month(spark):
    rows = [
        ("mod", "Defence", "2026-04", "Acme", 100.0, False),
        ("mod", "Defence", "2026-05", "Acme", 300.0, False),
        ("dhsc", "Health", "2026-04", "Beta", 40.0, False),
    ]
    g = {(r["source_department"], r["payment_month"]): r
         for r in P.aggregate_gold(spark.createDataFrame(rows, SCHEMA)).collect()}
    assert len(g) == 3
    assert str(g[("mod", "2026-05")]["total_spend"]) == "300.00"
    assert g[("dhsc", "2026-04")]["transaction_count"] == 1
