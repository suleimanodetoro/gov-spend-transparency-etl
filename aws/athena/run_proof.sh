#!/usr/bin/env bash
# ============================================================================
# Access-control proof: assume each demo role and run its query against the ONE
# governed table, capturing output to captures/aws/. Run AFTER stage 2
# (terraform apply -var enable_table_governance=true) and the Glue job.
#
#   analyst    : LF-Tag column governance — permitted columns OK, restricted column DENIED
#   assurance  : LF-Tag column governance — restricted columns OK, all rows
#   region-... : data filter row security — only one department's rows visible
#
# Requires: aws CLI, jq. Uses the operator's sts:AssumeRole on the three roles.
# ============================================================================
set -uo pipefail

REGION="${AWS_REGION:-eu-west-2}"
PROJECT="${PROJECT:-govspend}"
WG="${PROJECT}-workgroup"
DB="${GLUE_DATABASE:-gov_spend_assurance}"
TABLE="${GOVERNED_TABLE:-silver_clean_spend}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
OUT="$(cd "$(dirname "$0")/../.." && pwd)/captures/aws"
mkdir -p "$OUT"

assume_role() { # $1 = role short name (e.g. analyst-role)
  local creds
  creds="$(aws sts assume-role \
    --role-arn "arn:aws:iam::${ACCOUNT}:role/${PROJECT}-$1" \
    --role-session-name "proof-$1" --duration-seconds 3600 \
    --query Credentials --output json)" || return 1
  export AWS_ACCESS_KEY_ID="$(echo "$creds" | jq -r .AccessKeyId)"
  export AWS_SECRET_ACCESS_KEY="$(echo "$creds" | jq -r .SecretAccessKey)"
  export AWS_SESSION_TOKEN="$(echo "$creds" | jq -r .SessionToken)" # REQUIRED or InvalidClientTokenId
}
clear_creds() { unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN; }

run_query() { # $1 = SQL ; prints state + results (or the failure reason, which is the proof for the denied case)
  local qid st
  qid="$(aws athena start-query-execution --work-group "$WG" \
    --query-string "$1" \
    --query-execution-context "Database=${DB},Catalog=AwsDataCatalog" \
    --region "$REGION" --query QueryExecutionId --output text)" || { echo "start failed"; return 1; }
  while :; do
    st="$(aws athena get-query-execution --query-execution-id "$qid" --region "$REGION" \
          --query QueryExecution.Status.State --output text)"
    case "$st" in SUCCEEDED|FAILED|CANCELLED) break ;; esac
    sleep 2
  done
  echo "STATE: $st"
  if [ "$st" = SUCCEEDED ]; then
    aws athena get-query-results --query-execution-id "$qid" --region "$REGION" --output table
  else
    echo "REASON (this IS the proof for the denied case):"
    aws athena get-query-execution --query-execution-id "$qid" --region "$REGION" \
      --query QueryExecution.Status.StateChangeReason --output text
  fi
}

proof() { # $1 role  $2 label  $3 sql
  echo "==================================================================="
  echo "ROLE: $PROJECT-$1   PROOF: $2"
  echo "==================================================================="
  if ! assume_role "$1"; then echo "could not assume $1 (check sts:AssumeRole + trust policy)"; clear_creds; return; fi
  run_query "$3"
  clear_creds
}

{
proof analyst-role "01 analyst: permitted columns -> SUCCEEDS" \
  "SELECT source_department, payment_month, supplier_name, amount_gbp, expense_category, processing_status FROM ${DB}.${TABLE} ORDER BY payment_month LIMIT 20"

proof analyst-role "02 analyst: names restricted column 'approver' -> ACCESS DENIED" \
  "SELECT transaction_id, approver, risk_rating FROM ${DB}.${TABLE} LIMIT 20"

proof assurance-role "03 assurance: restricted columns -> SUCCEEDS, all rows" \
  "SELECT transaction_id, supplier_name, approver, budget_owner, risk_rating, internal_notes FROM ${DB}.${TABLE} LIMIT 20"

proof region-analyst-role "04 region-analyst: data filter -> only one department's rows" \
  "SELECT source_department, count(*) AS rows_visible, sum(amount_gbp) AS total_spend FROM ${DB}.${TABLE} GROUP BY source_department ORDER BY source_department"
} 2>&1 | tee "$OUT/athena_access_control_proof.log"

echo
echo "Saved: $OUT/athena_access_control_proof.log"
