# Government Spend Transparency ETL

A PySpark pipeline that turns messy departmental "spend over £25k" CSVs into clean,
deduplicated, analytics-ready tables, with quarantine for bad rows and an audit trail.

Runs locally on Parquet with no extra jars. The same logic maps to AWS Glue on
Iceberg in production; only the table format and catalog change.

## Run it

PySpark 4 needs Java 17 or 21. On macOS with Homebrew, pin it before running (Java 17 matches
AWS Glue 5.0): `export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`.

```bash
python3 -m venv .venv && source .venv/bin/activate   # recommended
make setup        # pyspark + pytest
make data         # generate messy sample CSVs into ./data/raw and reference data
make run          # bronze -> silver -> gold, plus quarantine + audit
make run          # again: every file is skipped (idempotent)
make test         # pytest
```

The second `make run` printing `[idempotency] skip already processed` for every file
is the live proof of restart-safety.

## Sources

- Departmental spend CSVs (messy: different column names, date formats, currency strings).
- Internal approvals ledger (cost centre, approver, budget owner, processing status, internal notes), sensitive.
- Supplier risk reference (category, risk rating, due diligence notes), sensitive.

## Layers (medallion)

- **bronze_raw** — every spend source conformed to one canonical schema, still untyped.
- **silver_clean_spend** — typed, validated, deduplicated on the natural key, joined to the
  approvals ledger and supplier risk reference. The governed assurance layer holding the
  restricted columns.
- **curated_open_analytics** — the analytics-safe view, restricted columns removed. In
  production this is the same Iceberg table with restricted columns hidden by Lake Formation
  for untagged principals, not a second physical table.
- **gold_department_monthly_metrics** — spend, transaction and supplier counts, credits, average.
- **rejected_records** — quarantined rows with a reason.
- **audit/** — processed-files manifest and a per-run summary.

Classification (`config/data_classification.json`) maps every column to public, internal or
restricted, which in production becomes the LF-Tag set governing column and row access (ABAC).

## What it demonstrates

1. **Source and outcome.** Three departments, three different shapes (column names, date
   formats, currency strings). Outcome: one canonical Parquet model, partitioned by
   `source_department` and `payment_month`, ready for analysis.
2. **Transformations.** Config-driven column mapping, tolerant `try_` parsing of amounts and
   dates (native functions, no Python UDFs), a broadcast-joined reference dimension, a stable
   `record_hash`, and dedup keeping the latest record.
3. **Performance.** Native functions over UDFs, broadcast join for the small dimension, AQE
   with skew handling, partition pruning, and dynamic partition overwrite so a reprocess
   replaces only the touched partitions.
4. **Errors, outages, schema evolution.** Bad rows are quarantined with a reason, never
   dropped, never failing the file. A file hash manifest makes re-runs idempotent and safe to
   retry. Renamed columns are absorbed by the versioned mapping; genuinely new columns (a VAT
   field, a Programme code) are captured to `raw_extras` and flagged, never silently dropped.
5. **Scaling.** Incremental by file, partition pruning keeps cost flat as history grows, and
   Spark scales horizontally. Nothing assumes the data fits on one node.

## Local vs production

| | Local (this repo) | Production |
|---|---|---|
| Engine | PySpark | AWS Glue (PySpark) |
| Storage | Parquet on disk | Iceberg tables on S3 |
| Catalog | filesystem | Glue Data Catalog |
| Access | physical open/restricted split | Lake Formation (LF-Tags + data filters) |
| Upsert | read-modify-write on affected partitions, keyed on `transaction_id` | Iceberg `MERGE INTO` on the natural key |

Parquet locally keeps it dependency-light and inspectable. The medallion logic is identical; in
production the writer targets Iceberg and the local read-modify-write upsert becomes an Iceberg
`MERGE INTO` on the natural key (`transaction_id`, composite with `source_department`), with
`record_hash` used only to detect changed rows. Iceberg, not Delta, because the target platform
is AWS Glue.

## AWS production lane

Provisioned as Terraform under [`aws/terraform/`](aws/terraform/) for AWS Glue 5.0
(Spark 3.5.4 / Iceberg 1.7.1):

- **Glue job** ([`aws/glue_job.py`](aws/glue_job.py)) imports the **same** `transforms.py` /
  `quality.py` (shipped via `--extra-py-files`) — one codebase, no copy. It writes Iceberg
  tables into the Glue Data Catalog and upserts silver with `MERGE INTO` on the natural key.
- **Lake Formation** governs the one silver table two ways: **LF-Tags** (column ABAC) for an
  `analyst` vs an `assurance` role, and a **data filter** (row/cell) for a `region-analyst`
  role — on separate principals, because a tag grant and a filter grant union and widen access.
- **Athena** (engine v3) proves it: `aws/athena/run_proof.sh` assumes each role and runs the
  four proof queries (permitted columns OK, restricted column denied, assurance sees all,
  region-analyst sees one department's rows).
- Deploy/teardown: [`aws/RUNBOOK.md`](aws/RUNBOOK.md) and [`aws/TEARDOWN.md`](aws/TEARDOWN.md).
  Only these services: S3, Glue, Lake Formation, Athena, CloudWatch, IAM, Budgets.

## What went well / what went wrong

**Well:** the quarantine pattern meant one malformed file never failed a run; the idempotent
manifest made a re-run a non-event; credits were preserved instead of being wrongly rejected.

**Wrong, then fixed:** Spark 4 throws on unparseable input rather than returning null, so the
first version died on a single bad row; switched to tolerant `try_to_timestamp` / `try_cast`. A
cross-file `transaction_id` collision inflated counts until a global dedup on the natural key was
added. The first incremental write used dynamic partition overwrite of only the new batch, which
would have dropped earlier rows if a late file landed in an existing department/month partition;
replaced with a read-modify-write upsert (the local stand-in for Iceberg `MERGE`), covered by an
incremental-load test. On Glue, `try_to_date` (Spark-4-only) had to become `try_to_timestamp`
(present in Spark 3.5), and the Iceberg `MERGE` source had to be staged because Spark rejects
non-deterministic expressions in a merge source.

## Layout

```
generate_sample_data.py     messy sample CSVs (stdlib only)
config/column_mappings.json versioned source-to-canonical mappings
src/transforms.py           read, normalise, clean, hash, enrich, dedupe
src/quality.py              validate and quarantine
src/pipeline.py             orchestration, idempotency manifest, medallion writes
tests/                      pytest: parsing, mapping, quality, idempotency, incremental upsert, hybrid
aws/terraform/              S3, Glue, Lake Formation, IAM, Athena, Budgets (all IaC)
aws/glue_job.py             Glue 5.0 entrypoint importing the shared modules
aws/athena/run_proof.sh     assume each role + run the access-control proof queries
aws/RUNBOOK.md, TEARDOWN.md deploy + teardown
captures/                   console proof (local + AWS)
.github/workflows/ci.yml    pytest + terraform fmt/validate on push
Makefile                    setup / data / run / rerun / test / clean
```
