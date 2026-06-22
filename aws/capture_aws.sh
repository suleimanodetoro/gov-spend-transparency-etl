#!/usr/bin/env bash
# Collect AWS-lane proof artefacts into captures/aws/ (text captures; this CLI has no GUI).
# Run after the Glue job SUCCEEDS and stage-2 governance is applied.
set -uo pipefail
REGION="${AWS_REGION:-eu-west-2}"
JOB="gov-spend-assurance-etl"
DB="gov_spend_assurance"
CUR="govspend-curated-556524450848"
OUT="$(cd "$(dirname "$0")/.." && pwd)/captures/aws"
mkdir -p "$OUT"

echo "# Glue job run" | tee "$OUT/glue_job_run.txt"
RUN_ID="$(cat /tmp/glue_run_id.txt 2>/dev/null)"
aws glue get-job-run --job-name "$JOB" --run-id "$RUN_ID" --region "$REGION" \
  --query 'JobRun.{State:JobRunState,ExecutionSeconds:ExecutionTime,Started:StartedOn,Workers:NumberOfWorkers,GlueVersion:GlueVersion}' \
  --output table | tee -a "$OUT/glue_job_run.txt"

echo "# Iceberg tables in the Glue Data Catalog" | tee "$OUT/glue_tables.txt"
aws glue get-tables --database-name "$DB" --region "$REGION" \
  --query 'TableList[].{Table:Name,Type:Parameters.table_type,Format:Parameters.format}' \
  --output table | tee -a "$OUT/glue_tables.txt"

echo "# S3 Iceberg layout for silver_clean_spend (metadata/ + data/)" | tee "$OUT/s3_iceberg_layout.txt"
aws s3 ls "s3://$CUR/warehouse/silver_clean_spend/" --recursive --region "$REGION" \
  | awk '{print $4}' | sed 's#warehouse/silver_clean_spend/##' | head -40 | tee -a "$OUT/s3_iceberg_layout.txt"

echo "# CloudWatch custom metrics (GovSpendETL namespace)" | tee "$OUT/cloudwatch_metrics.txt"
aws cloudwatch list-metrics --namespace GovSpendETL --region "$REGION" \
  --query 'Metrics[].MetricName' --output table | tee -a "$OUT/cloudwatch_metrics.txt"

echo "# Glue job summary log line (valid/rejected)" | tee "$OUT/glue_summary_log.txt"
LG="/aws-glue/jobs/output"
STREAMS=$(aws logs describe-log-streams --log-group-name "$LG" --region "$REGION" \
  --order-by LastEventTime --descending --max-items 5 --query 'logStreams[].logStreamName' --output text 2>/dev/null)
for s in $STREAMS; do
  aws logs get-log-events --log-group-name "$LG" --log-stream-name "$s" --region "$REGION" \
    --query 'events[].message' --output text 2>/dev/null | grep -E '\[summary\]|\[ok\]|valid=' && break
done | tee -a "$OUT/glue_summary_log.txt"

echo
echo "Saved captures to $OUT"
ls -1 "$OUT"
