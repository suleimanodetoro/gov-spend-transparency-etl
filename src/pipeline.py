"""
Orchestration for the government spend pipeline.

Layers: bronze (conformed, untyped) -> silver (clean, validated, deduped) -> gold
(department-month metrics), plus a quarantine table and an audit manifest.

Idempotency: each file is hashed and recorded in a manifest; a re-run skips files
already processed. Silver is written with dynamic partition overwrite, so reprocessing
a partition replaces it rather than appending. In production on Iceberg, the same
identity logic upgrades to a row-level MERGE.

Local format is Parquet (no extra jars, runs anywhere). Production format is Iceberg
on S3 in the Glue Data Catalog, governed by Lake Formation. Only the table format and
catalog config change; the medallion logic is identical.
"""
import csv, glob, hashlib, json, os, shutil
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import transforms as T
import quality as Q

SILVER_COLS = ["record_hash", "transaction_id", "source_department", "department_group",
               "payment_month", "payment_date", "supplier_name", "amount_gbp",
               "vat_amount_gbp", "expense_category", "needs_review", "raw_extras",
               "schema_version", "source_file", "ingested_at"]

# Columns classified "restricted": present in the governed assurance layer, removed from
# the open analytics layer. In production these are governed by Lake Formation LF-Tags.
RESTRICTED = ["approver", "budget_owner", "internal_notes", "risk_rating", "due_diligence_notes"]


# ----------------------------- audit / manifest ----------------------------- #
def file_hash(path):
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def load_manifest(path):
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return {row["file_hash"] for row in csv.DictReader(f)}


def append_manifest(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["source_file", "file_hash", "rows_valid", "rows_rejected", "processed_at"])
        w.writerows(rows)


def source_key(path):
    return os.path.basename(path).split("_spend_")[0]


# ----------------------------- spark ----------------------------- #
def build_spark():
    return (SparkSession.builder.appName("gov-spend-transparency-etl")
            .master("local[2]")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate())


def aggregate_gold(silver):
    return (silver.groupBy("source_department", "department_group", "payment_month")
            .agg(F.sum("amount_gbp").cast("decimal(18,2)").alias("total_spend"),
                 F.count("*").alias("transaction_count"),
                 F.countDistinct("supplier_name").alias("supplier_count"),
                 F.sum(F.col("needs_review").cast("int")).alias("credit_count"),
                 F.avg("amount_gbp").cast("decimal(18,2)").alias("avg_amount"))
            .orderBy("source_department", "payment_month"))


def upsert_silver(spark, silver_path, new_silver):
    """Idempotent upsert for the local Parquet silver layer. Reads any existing silver, unions
    the new batch, deduplicates on the natural key (latest ingested_at wins), and rewrites via a
    staging directory swap. This means a late file landing in an existing department/month
    partition ADDS its rows without clobbering the rows already there. Production upgrades this
    to a single Iceberg MERGE INTO on the natural key."""
    if os.path.exists(silver_path):
        combined = spark.read.parquet(silver_path).unionByName(new_silver, allowMissingColumns=True)
    else:
        combined = new_silver
    merged = T.dedup(combined)
    staging = silver_path + "__staging"
    shutil.rmtree(staging, ignore_errors=True)
    merged.write.mode("overwrite").partitionBy("source_department", "payment_month").parquet(staging)
    shutil.rmtree(silver_path, ignore_errors=True)
    shutil.move(staging, silver_path)


def main():
    base = os.getcwd()
    out = os.path.join(base, "output")
    paths = {"bronze": f"{out}/bronze_raw", "silver": f"{out}/silver_clean_spend",
             "open": f"{out}/curated_open_analytics",
             "gold": f"{out}/gold_department_monthly_metrics", "rejected": f"{out}/rejected_records",
             "manifest": f"{out}/audit/processed_files_manifest.csv",
             "summary": f"{out}/audit/run_summary.json"}

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    cfg = json.load(open(os.path.join(base, "config", "column_mappings.json")))
    version, mappings = cfg["schema_version"], cfg["sources"]

    seen = load_manifest(paths["manifest"])
    files = sorted(glob.glob(os.path.join(base, "data", "raw", "*.csv")))
    if not files:
        print("No files in ./data/raw. Run: python generate_sample_data.py")
        return

    norm_frames, silver_frames, rejected_frames, summary, manifest_rows = [], [], [], [], []
    for fp in files:
        h = file_hash(fp)
        if h in seen:
            print(f"[idempotency] skip already processed: {os.path.basename(fp)}")
            continue
        key = source_key(fp)
        if key not in mappings:
            print(f"[error] no mapping for source '{key}' ({os.path.basename(fp)}); parked")
            continue

        norm = T.normalise(T.read_raw(spark, fp), mappings[key], key, version)
        enriched = T.enrich(spark, T.add_record_hash(T.clean(norm)))
        valid, rejected = Q.split_valid_quarantine(enriched)
        valid = T.dedup(valid)

        valid = valid.cache()
        nv, nr = valid.count(), rejected.count()
        norm_frames.append(norm); silver_frames.append(valid.select(*SILVER_COLS))
        rejected_frames.append(rejected)
        summary.append({"file": os.path.basename(fp), "department": key, "valid": nv, "rejected": nr})
        manifest_rows.append([os.path.basename(fp), h, nv, nr, datetime.now(timezone.utc).isoformat()])
        print(f"[ok] {os.path.basename(fp):42s} valid={nv:4d}  quarantined={nr}")

    if not silver_frames:
        print("\nNothing new to process. Pipeline is idempotent.")
        if os.path.exists(paths["gold"]):
            spark.read.parquet(paths["gold"]).show(50, truncate=False)
        spark.stop(); return

    base_silver = T.dedup(reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), silver_frames))

    reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), norm_frames) \
        .write.mode("overwrite").partitionBy("source_department").parquet(paths["bronze"])
    reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), rejected_frames) \
        .write.mode("append").parquet(paths["rejected"])

    # Hybrid: attach the internal approvals ledger and supplier risk reference.
    ref = os.path.join(base, "data", "reference")
    approvals = spark.read.option("header", True).csv(f"{ref}/approvals_ledger.csv").dropDuplicates(["transaction_id"])
    suppliers = (spark.read.option("header", True).csv(f"{ref}/supplier_reference.csv")
                 .withColumn("supplier_name", F.initcap(F.trim(F.col("supplier_name")))))
    silver_full = T.attach_assurance(spark, base_silver, approvals, suppliers)

    # silver = governed assurance layer. Upsert so a late file landing in an existing
    # partition adds rows without clobbering earlier ones (the local stand-in for Iceberg MERGE).
    upsert_silver(spark, paths["silver"], silver_full)

    # open and gold are fully re-derived from the merged silver each run.
    final_silver = spark.read.parquet(paths["silver"])
    T.open_layer(final_silver, RESTRICTED) \
        .write.mode("overwrite").partitionBy("source_department", "payment_month").parquet(paths["open"])
    aggregate_gold(spark.read.parquet(paths["open"])).write.mode("overwrite").parquet(paths["gold"])

    append_manifest(paths["manifest"], manifest_rows)
    os.makedirs(os.path.dirname(paths["summary"]), exist_ok=True)
    json.dump({"run_at": datetime.now(timezone.utc).isoformat(),
               "files": summary,
               "total_valid": sum(s["valid"] for s in summary),
               "total_rejected": sum(s["rejected"] for s in summary)},
              open(paths["summary"], "w"), indent=2)

    print("\n=== GOLD: department monthly metrics ===")
    spark.read.parquet(paths["gold"]).show(50, truncate=False)
    print("=== Quarantine sample ===")
    spark.read.parquet(paths["rejected"]).select(
        "source_file", "transaction_id", "raw_amount", "raw_date", "rejection_reason"
    ).show(20, truncate=False)
    print("=== ABAC: open analytics layer columns (restricted removed) ===")
    print(sorted(spark.read.parquet(paths["open"]).columns))
    print("=== ABAC: restricted columns live only in the assurance layer ===")
    print(sorted(c for c in spark.read.parquet(paths["silver"]).columns if c in RESTRICTED))
    spark.stop()


if __name__ == "__main__":
    main()
