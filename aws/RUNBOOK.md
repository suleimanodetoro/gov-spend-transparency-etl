# RUNBOOK — AWS lane (Glue + Iceberg + Lake Formation + Athena)

Deploys the governed Iceberg lakehouse and proves Lake Formation access control. Every billable
step is called out. Region is **eu-west-2** throughout.

> **Sandbox-account warning.** `aws_lakeformation_data_lake_settings` is **account-wide and
> authoritative** — applying it sets the Lake Formation admins to `aws-sda-user` and clears the
> `IAMAllowedPrincipals` default so LF actually enforces on new tables. If this account uses Lake
> Formation for anything else, that will be overwritten. Only run this in a sandbox account.

## Prerequisites
```bash
export AWS_REGION=eu-west-2 AWS_DEFAULT_REGION=eu-west-2
aws sts get-caller-identity                 # expect account 556524450848, user aws-sda-user
cd <repo>
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
./.venv/bin/python generate_sample_data.py  # data/ must exist; Terraform uploads it
```

## Step 0 — Budget FIRST (billable guard, ~£0.02/day after the free 2 budgets)
The budget is created before anything else so the whole demo is guarded.
```bash
cd aws/terraform
terraform init
terraform apply -target=aws_budgets_budget.monthly       # review, then approve
```

## Step 1 — Stage-1 infrastructure (billable: S3, Glue, IAM, Athena, LF setup)
```bash
terraform plan -out tf.plan          # review: ~53 resources, all S3/Glue/LF/Athena/IAM/Budgets
terraform apply tf.plan              # approve
```
What this creates: 4 S3 buckets (+ data/config/script/src.zip uploads), the Glue Data Catalog
database, the Glue job definition, IAM (1 writer + 3 consumer roles), the Athena engine-v3
workgroup + 4 named proof queries, and the **stage-1** Lake Formation setup (data-lake admin,
registered curated location, the `classification` LF-Tag, the database baseline tag, and the
writer's LF grants). It does NOT yet tag columns or create the data filter — those need the
tables, which the Glue job creates next.

## Step 2 — Run the Glue job (billable: Glue DPU-hours; minutes on G.1X×2)
```bash
aws glue start-job-run --job-name gov-spend-assurance-etl --region eu-west-2
# watch:
aws glue get-job-runs --job-name gov-spend-assurance-etl --region eu-west-2 \
  --query 'JobRuns[0].{State:JobRunState,Started:StartedOn,Error:ErrorMessage}'
```
On success the Glue Data Catalog holds four Iceberg tables: `silver_clean_spend`,
`curated_open_analytics`, `gold_department_metrics`, `rejected_records`.
If it fails, read the CloudWatch log group `/aws-glue/jobs/error` and iterate — likely first-run
causes: a Lake Formation grant missing for the writer, or an Iceberg/FTA Spark-config mismatch.

## Step 3 — Stage-2 table governance (re-apply with the flag on)
```bash
terraform apply -var enable_table_governance=true    # review, approve
```
Adds: column LF-Tag assignments (public/internal/restricted per data_classification.json), the
`region-analyst` data cells filter (rows where `source_department = 'mod'`), and the consumer
grants (analyst & assurance via LF-Tags; region-analyst via the data filter ONLY).

## Step 4 — Prove access control (billable: tiny Athena scans)
```bash
cd ../athena
chmod +x run_proof.sh
./run_proof.sh        # assumes each role, runs its query, writes captures/aws/athena_access_control_proof.log
```
Expected:
1. analyst, permitted columns → **SUCCEEDS**.
2. analyst, names `approver` (restricted) → **ACCESS DENIED** (column governance proof).
3. assurance, restricted columns → **SUCCEEDS**, all rows.
4. region-analyst, group by department → **only `mod`** rows (data filter / row security proof).

The pair (2 denied) vs (3 allowed) proves LF-Tag column governance; (4) proves the data filter.

## Step 5 — Captures (see CAPTURE CHECKLIST below), then TEARDOWN
Tear down the moment captures are saved — see `aws/TEARDOWN.md`.

---

## CAPTURE CHECKLIST (save under captures/aws/)
- [ ] Glue job run = **Succeeded** (console Runs tab) + the CloudWatch `[summary]` log line.
- [ ] The four Iceberg tables in the Glue Data Catalog (table type = ICEBERG).
- [ ] S3 curated layout showing Iceberg `metadata/` + `data/` for `silver_clean_spend`.
- [ ] CloudWatch metric `GovSpendETL/ValidRecords` & `RejectedRecords`.
- [ ] `athena_access_control_proof.log` (produced by run_proof.sh) — the four results.
- [ ] Side-by-side: query 2 (denied) next to query 3 (allowed) = column governance.
- [ ] Query 4 result = row filter.

## Idempotency / re-run note
Re-running the Glue job is safe: silver is upserted by Iceberg `MERGE INTO` on the natural key
(`transaction_id`, `source_department`); `record_hash` gates the UPDATE so unchanged rows are
skipped and corrections update in place. The other tables are fully re-derived each run.

## Known footguns (from the doc research, so first apply is calm)
- LF `data_lake_settings` is authoritative — manage all its settings in this one resource.
- Do not grant LF permissions to an admin principal (we don't; admin = aws-sda-user, grantees =
  the 4 non-admin roles).
- A tag grant + a data-filter grant on ONE principal UNION and defeat the filter — kept separate.
- Athena `SELECT *` silently drops ungranted columns; the denied proof must NAME a restricted column.
- Athena cannot do Iceberg DDL on an LF-registered location — DDL is the Glue job's job, queries are SELECT-only.
- If a consumer query errors on the `default` database, grant it `DESCRIBE` on `default` (see lakeformation_cli.sh).
