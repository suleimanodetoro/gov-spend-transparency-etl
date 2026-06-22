"""
Transform stage. Native Spark functions only (no Python UDFs), so work pushes down
and stays in the JVM. Parsing helpers are exposed for unit testing.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

DATE_FORMATS = ["dd/MM/yyyy", "yyyy-MM-dd", "dd-MMM-yyyy", "d/M/yyyy"]

CANONICAL = ["transaction_id", "supplier_name", "payment_date",
             "amount", "expense_category", "vat_amount"]

# small reference dim for a broadcast join (no shuffle for a lookup)
DEPT_GROUP = {"cabinet_office": "Cabinet Office", "mod": "Defence", "dhsc": "Health"}


def parse_amount(colname):
    """'£1,234.56' / '1,250' / '(500.00)' -> double. '(...)' is a credit (negative).
    try_cast tolerates junk like 'INVALID' by returning NULL instead of failing."""
    return F.expr(
        "try_cast(regexp_replace(trim(`{c}`), '[^0-9.-]', '') as double) "
        "* (case when trim(`{c}`) like '(%)' then -1 else 1 end)".format(c=colname)
    )


def parse_date(colname):
    """Try each accepted format; first that parses wins. Returns NULL on unparseable or invalid
    dates (e.g. 31/02), so bad dates are quarantined downstream instead of throwing under ANSI.

    Uses try_to_timestamp (added in Spark 3.5.0, so present on BOTH Glue 5.0's Spark 3.5.4 and
    local Spark 4.x) cast to date. NOTE: the more obvious try_to_date is Spark-4-only and is
    UNRESOLVED on Glue 5.0 — using it here is what broke the first Glue run."""
    exprs = ", ".join("try_to_timestamp(`{c}`, '{f}')".format(c=colname, f=fmt) for fmt in DATE_FORMATS)
    return F.expr("to_date(coalesce({}))".format(exprs))


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    df = (spark.read.option("header", True).option("encoding", "UTF-8")
          .csv(path, inferSchema=False))
    return df.withColumn("source_file", F.input_file_name())


def normalise(df: DataFrame, mapping: dict, source_department: str, version: str) -> DataFrame:
    """Rename incoming columns to canonical via config; capture unmapped columns to a
    raw_extras map (flagged, never dropped) so schema drift cannot break the job."""
    incoming = [col for col in df.columns if col != "source_file"]
    mapped = {src: canon for src, canon in mapping.items() if src in incoming}
    unmapped = [col for col in incoming if col not in mapping]
    if unmapped:
        print(f"[schema-evolution] {source_department}: unmapped -> raw_extras: {unmapped}")

    exprs = [F.col(src).alias(canon) for src, canon in mapped.items()]
    if unmapped:
        kv = []
        for col in unmapped:
            kv += [F.lit(col), F.col(col).cast(StringType())]
        exprs.append(F.create_map(*kv).alias("raw_extras"))
    else:
        exprs.append(F.expr("map()").cast("map<string,string>").alias("raw_extras"))
    out = df.select(*exprs, F.col("source_file"))

    for canon in CANONICAL:
        if canon not in out.columns:
            out = out.withColumn(canon, F.lit(None).cast(StringType()))
    return (out.withColumn("source_department", F.lit(source_department))
               .withColumn("schema_version", F.lit(version)))


def clean(df: DataFrame) -> DataFrame:
    return (df
        .withColumn("amount_raw", F.col("amount"))
        .withColumn("payment_date_raw", F.col("payment_date"))
        .withColumn("amount_gbp", parse_amount("amount"))
        .withColumn("vat_amount_gbp", parse_amount("vat_amount"))
        .withColumn("payment_date", parse_date("payment_date"))
        .withColumn("payment_month", F.date_format(parse_date("payment_date_raw"), "yyyy-MM"))
        .withColumn("supplier_name", F.initcap(F.trim(F.col("supplier_name"))))
        .withColumn("needs_review", F.coalesce(F.col("amount_gbp") < 0, F.lit(False)))
        .withColumn("ingested_at", F.current_timestamp())
        .drop("amount", "vat_amount"))


def add_record_hash(df: DataFrame) -> DataFrame:
    """Content hash over the natural key plus mutable fields (date, amount). Use it to detect
    whether a matched row's content changed, NOT as a stable identity: the hash changes when a
    transaction is corrected. The stable key for dedup and for production MERGE is
    transaction_id (composite with source_department)."""
    return df.withColumn("record_hash", F.sha2(F.concat_ws("|",
        F.coalesce(F.col("transaction_id"), F.lit("")),
        F.col("source_department"),
        F.coalesce(F.col("payment_date").cast("string"), F.lit("")),
        F.coalesce(F.col("amount_gbp").cast("string"), F.lit(""))), 256))


def enrich(spark: SparkSession, df: DataFrame) -> DataFrame:
    ref = spark.createDataFrame(list(DEPT_GROUP.items()), ["dept_key", "department_group"])
    return df.join(F.broadcast(ref), df["source_department"] == ref["dept_key"], "left").drop("dept_key")


def dedup(df: DataFrame) -> DataFrame:
    key = F.coalesce(F.col("transaction_id"), F.col("record_hash"))
    w = Window.partitionBy(key).orderBy(F.col("ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def attach_assurance(spark: SparkSession, silver: DataFrame, approvals: DataFrame, suppliers: DataFrame) -> DataFrame:
    """Join the internal approvals ledger and supplier risk reference. These add the
    sensitive columns (approver, internal notes, risk rating) that the open analytics
    layer must not expose and that Lake Formation ABAC governs in production."""
    return (silver
            .join(approvals, "transaction_id", "left")
            .join(F.broadcast(suppliers), "supplier_name", "left")
            .withColumn("missing_approval", F.col("processing_status").isNull()))


def open_layer(df: DataFrame, restricted_cols) -> DataFrame:
    """Analytics-safe view: restricted columns removed. In production this is the same
    Iceberg table with restricted columns hidden by Lake Formation for untagged
    principals, not a second physical table."""
    return df.select(*[c for c in df.columns if c not in restricted_cols])
