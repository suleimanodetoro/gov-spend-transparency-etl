"""
AWS Glue 5.0 entrypoint for the Government Spend Assurance ETL.

This is a THIN orchestrator. All real logic lives in the SAME modules the local
pipeline and the unit tests use — `transforms.py` and `quality.py` — shipped to the
job via `--extra-py-files s3://<scripts>/scripts/src.zip`. There is no second copy of
the transform logic; only the I/O boundary and the table format differ from local.

Local vs production, side by side:
  - Local : Parquet on disk, a file-hash manifest for idempotency, a read-modify-write
            upsert as the stand-in for a row-level merge.
  - Glue  : Apache Iceberg tables in the Glue Data Catalog on S3, governed by Lake
            Formation, idempotent via Iceberg `MERGE INTO` on the natural key.

Idempotency (the production upgrade over the local manifest): the job reprocesses the
raw prefix and MERGEs into the governed silver table on the STABLE natural key
(transaction_id, source_department). `record_hash` is used ONLY to decide whether a
matched row actually changed, so unchanged rows are skipped and corrections update in
place instead of inserting a duplicate. Re-running the job is therefore a no-op on
unchanged data. (At higher volume, Glue job bookmarks would also skip already-read S3
objects — the direct analogue of the local file manifest — but MERGE alone already
makes the run safe to retry.)
"""
import sys
import json
from datetime import datetime, timezone
from functools import reduce

import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Shared logic — identical to the local lane (one codebase).
import transforms as T
import quality as Q

# Canonical silver column contract (must match the local silver projection exactly).
SILVER_COLS = ["record_hash", "transaction_id", "source_department", "department_group",
               "payment_month", "payment_date", "supplier_name", "amount_gbp",
               "vat_amount_gbp", "expense_category", "needs_review", "raw_extras",
               "schema_version", "source_file", "ingested_at"]


# ------------------------------------------------------------------ helpers -- #
def source_key(s3_key: str) -> str:
    """cabinet_office from .../spend/cabinet_office_spend_2026_05_schemadrift.csv —
    same rule as the local pipeline so the right mapping is selected per file."""
    return s3_key.rsplit("/", 1)[-1].split("_spend_")[0]


def read_json_s3(s3, bucket: str, key: str) -> dict:
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def list_spend_files(s3, bucket: str, prefix: str = "spend/"):
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".csv")]
    return sorted(keys)


def ensure_table(spark, fqn: str, source_view: str, partitioned: bool):
    """Create the Iceberg table once, with the right schema and partitioning, empty.
    `IF NOT EXISTS` + a `WHERE 1=0` CTAS makes this idempotent: first run creates the
    schema, later runs are no-ops and fall straight through to the MERGE."""
    part = "PARTITIONED BY (source_department, payment_month)" if partitioned else ""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fqn}
        USING iceberg {part}
        AS SELECT * FROM {source_view} WHERE 1=0
    """)


def merge_on_natural_key(spark, fqn: str, source_view: str):
    """Row-level upsert keyed on the STABLE natural key. record_hash gates the UPDATE so
    unchanged rows are skipped; a corrected upstream row (new hash) updates in place
    rather than inserting a duplicate."""
    spark.sql(f"""
        MERGE INTO {fqn} t
        USING {source_view} s
          ON  t.transaction_id   = s.transaction_id
          AND t.source_department = s.source_department
        WHEN MATCHED AND t.record_hash <> s.record_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def emit_metric(region: str, valid: int, rejected: int):
    """Custom CloudWatch metric so valid/rejected volumes per run are observable and
    alertable (a spike in rejects is the signal an upstream schema changed)."""
    cw = boto3.client("cloudwatch", region_name=region)
    now = datetime.now(timezone.utc)
    cw.put_metric_data(
        Namespace="GovSpendETL",
        MetricData=[
            {"MetricName": "ValidRecords", "Value": float(valid), "Unit": "Count", "Timestamp": now},
            {"MetricName": "RejectedRecords", "Value": float(rejected), "Unit": "Count", "Timestamp": now},
        ],
    )


# --------------------------------------------------------------------- main -- #
def build_spark(args):
    """Configure the Iceberg Glue catalog and Lake Formation in code via SparkConf, rather
    than as a single chained --conf job parameter. The job parameter form requires every key
    after the first to be re-prefixed with '--conf ' inside one string; getting that join
    wrong silently drops settings. SparkConf is unambiguous and self-documenting. The jars
    themselves are still loaded by the '--datalake-formats=iceberg' job parameter.

    The catalog uses the Glue Data Catalog for metadata (GlueCatalog) and S3FileIO for data.
    `spark.sql.extensions` MUST be present at SparkContext creation for MERGE INTO / UPDATE /
    DELETE and partition transforms to parse — which is exactly why it is set here, before the
    context exists, not afterwards on an existing session.

    Lake Formation: the job WRITES Iceberg tables, and Glue 5.0 fine-grained access control
    (FGAC) does not support writes, so the writer uses Full Table Access (FTA) credential
    vending (`glue.lakeformation-enabled=true`). The three directory flags are required so
    CREATE/DROP against an LF-registered location succeed (LF credentials are only vended
    after the catalog operation). Read governance for analysts is enforced separately at
    query time by Lake Formation (LF-Tags + data filters), not in this writer.
    """
    cat = args["catalog"]
    conf = SparkConf()
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{cat}.warehouse", args["warehouse"])
    conf.set(f"spark.sql.catalog.{cat}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set(f"spark.sql.catalog.{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

    # Glue 5.0 S3A defaults to us-east-2 if no region is set, which breaks --TempDir writes in
    # another region; pin it. (Iceberg uses S3FileIO, but Spark's temp/shuffle paths use S3A.)
    conf.set("spark.hadoop.fs.s3a.endpoint.region", args["region"])

    # Full Table Access credential vending is OFF by default: Iceberg S3FileIO writes data and
    # metadata with the task role's own credentials anyway (LF vending doesn't cover the FileIO
    # write path), so the writer uses direct S3 to the curated location (granted in IAM) under
    # the location's Lake Formation HYBRID access mode. Read governance is unaffected — analysts
    # have no direct S3 on curated and are governed by LF at query time.
    if args.get("lakeformation_enabled", "false").lower() == "true":
        conf.set(f"spark.sql.catalog.{cat}.glue.lakeformation-enabled", "true")
        conf.set(f"spark.sql.catalog.{cat}.client.region", args["region"])
        conf.set(f"spark.sql.catalog.{cat}.glue.account-id", args["account_id"])
        conf.set("spark.sql.catalog.skipLocationValidationOnCreateTable.enabled", "true")
        conf.set("spark.sql.catalog.createDirectoryAfterTable.enabled", "true")
        conf.set("spark.sql.catalog.dropDirectoryBeforeTable.enabled", "true")

    sc = SparkContext(conf=conf)
    glue = GlueContext(sc)
    return glue


def main():
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME", "raw_bucket", "curated_bucket", "scripts_bucket",
        "database", "region", "catalog", "warehouse", "account_id",
        "lakeformation_enabled",
    ])

    glue = build_spark(args)
    spark = glue.spark_session
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    catalog, db = args["catalog"], args["database"]
    raw_bucket, curated_bucket, scripts_bucket = args["raw_bucket"], args["curated_bucket"], args["scripts_bucket"]
    region = args["region"]

    def tbl(name):
        return f"{catalog}.{db}.{name}"

    s3 = boto3.client("s3", region_name=region)

    # Config from the scripts bucket — same JSON the local lane uses.
    mapping_cfg = read_json_s3(s3, scripts_bucket, "config/column_mappings.json")
    classification = read_json_s3(s3, scripts_bucket, "config/data_classification.json")
    version, mappings = mapping_cfg["schema_version"], mapping_cfg["sources"]

    # RESTRICTED is DERIVED from the classification config (the same file that becomes the
    # Lake Formation LF-Tags), so the open-layer split and the column governance share one
    # source of truth instead of a hand-maintained list.
    restricted = [c for c, level in classification["columns"].items() if level == "restricted"]

    # Per-file read + normalise (different departments have different schemas, and the
    # cabinet_office schema-drift file has extra columns, so files are read individually
    # exactly as locally; at scale you would batch by stable (source, schema_version)).
    files = list_spend_files(s3, raw_bucket)
    silver_frames, rejected_frames, total_valid, total_rejected = [], [], 0, 0

    for key in files:
        dept = source_key(key)
        if dept not in mappings:
            print(f"[error] no mapping for source '{dept}' ({key}); parked")
            continue
        path = f"s3://{raw_bucket}/{key}"
        norm = T.normalise(T.read_raw(spark, path), mappings[dept], dept, version)
        enriched = T.enrich(spark, T.add_record_hash(T.clean(norm)))
        valid, rejected = Q.split_valid_quarantine(enriched)
        valid = T.dedup(valid).select(*SILVER_COLS).cache()
        nv, nr = valid.count(), rejected.count()
        total_valid += nv
        total_rejected += nr
        silver_frames.append(valid)
        rejected_frames.append(rejected)
        print(f"[ok] {key:50s} valid={nv:5d}  quarantined={nr}")

    if not silver_frames:
        print("No spend files found under raw/spend/. Nothing to do.")
        job.commit()
        return

    # Global dedup across files on the natural key (cross-file collisions must not inflate
    # counts, and must be resolved BEFORE the approvals join multiplies them).
    base_silver = T.dedup(reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), silver_frames))

    # Attach the sensitive assurance sources (approvals ledger + supplier risk). Dedup the
    # approvals ledger on transaction_id first, or the left join fans out duplicates.
    ref = f"s3://{raw_bucket}/reference"
    approvals = (spark.read.option("header", True).csv(f"{ref}/approvals_ledger.csv")
                 .dropDuplicates(["transaction_id"]))
    suppliers = (spark.read.option("header", True).csv(f"{ref}/supplier_reference.csv")
                 .withColumn("supplier_name", F.initcap(F.trim(F.col("supplier_name")))))
    silver_full = T.attach_assurance(spark, base_silver, approvals, suppliers)

    # ---- Iceberg writes into the governed catalog ----
    # Stage the deduped, assurance-joined source to Parquet and read it back BEFORE the MERGE.
    # The in-memory source plan contains non-deterministic expressions (current_timestamp for
    # ingested_at, input_file_name for source_file) and Spark rejects non-deterministic sources
    # inside a MERGE (the source is scanned twice). The round-trip freezes them to data.
    stage_path = f"s3://{scripts_bucket}/tmp/silver_stage/"
    silver_full.write.mode("overwrite").parquet(stage_path)
    spark.read.parquet(stage_path).createOrReplaceTempView("src_silver")
    ensure_table(spark, tbl("silver_clean_spend"), "src_silver", partitioned=True)
    merge_on_natural_key(spark, tbl("silver_clean_spend"), "src_silver")

    # curated_open_analytics: the analytics-safe physical table (restricted columns dropped).
    # Fully re-derived from the merged silver each run, so a plain create-or-replace is
    # idempotent by construction. In production this same cut is ALSO enforced on the silver
    # table itself by Lake Formation LF-Tags for untagged principals.
    open_cols = [c for c in spark.table(tbl("silver_clean_spend")).columns if c not in restricted]
    open_df = spark.table(tbl("silver_clean_spend")).select(*open_cols)
    open_df.writeTo(tbl("curated_open_analytics")).using("iceberg") \
        .partitionedBy(F.col("source_department"), F.col("payment_month")).createOrReplace()

    # gold metrics — fully derived, create-or-replace.
    gold = (spark.table(tbl("curated_open_analytics"))
            .groupBy("source_department", "department_group", "payment_month")
            .agg(F.sum("amount_gbp").cast("decimal(18,2)").alias("total_spend"),
                 F.count("*").alias("transaction_count"),
                 F.countDistinct("supplier_name").alias("supplier_count"),
                 F.sum(F.col("needs_review").cast("int")).alias("credit_count"),
                 F.avg("amount_gbp").cast("decimal(18,2)").alias("avg_amount")))
    gold.writeTo(tbl("gold_department_metrics")).using("iceberg").createOrReplace()

    # rejected_records — also fully recomputed each run (all raw is reprocessed), so
    # create-or-replace keeps it idempotent rather than appending duplicates on retry.
    rejected_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), rejected_frames)
    rejected_all.writeTo(tbl("rejected_records")).using("iceberg").createOrReplace()

    print(f"[summary] valid={total_valid} rejected={total_rejected} "
          f"tables=[silver_clean_spend, curated_open_analytics, gold_department_metrics, rejected_records]")
    emit_metric(region, total_valid, total_rejected)

    job.commit()


if __name__ == "__main__":
    main()
