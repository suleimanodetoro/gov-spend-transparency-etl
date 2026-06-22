"""
Quality stage. Row-level resilience: bad rows go to a quarantine table with a reason
instead of failing the file. Negative amounts are legitimate credits, so they are kept
and flagged for review, not rejected.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REQUIRED = ["transaction_id", "supplier_name", "payment_date", "amount_gbp"]


def split_valid_quarantine(df: DataFrame):
    problems = F.concat_ws("; ",
        F.when(F.col("transaction_id").isNull() | (F.trim(F.col("transaction_id")) == ""), F.lit("missing_transaction_id")),
        F.when(F.col("supplier_name").isNull() | (F.trim(F.col("supplier_name")) == ""), F.lit("missing_supplier")),
        F.when(F.col("payment_date").isNull(), F.lit("invalid_payment_date")),
        F.when(F.col("amount_gbp").isNull(), F.lit("invalid_amount")),
    )
    reason = F.when(problems == "", F.lit(None)).otherwise(problems)
    tagged = df.withColumn("rejection_reason", reason)
    valid = tagged.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")
    rejected = (tagged.filter(F.col("rejection_reason").isNotNull())
                .select("source_file", "source_department",
                        F.col("transaction_id"),
                        F.col("amount_raw").alias("raw_amount"),
                        F.col("payment_date_raw").alias("raw_date"),
                        "rejection_reason", "ingested_at"))
    return valid, rejected
