#!/usr/bin/env bash
# ============================================================================
# Lake Formation CLI FALLBACK + contingencies.
#
# Lake Formation governance is managed in Terraform (lakeformation.tf). This script is the
# documented `aws lakeformation` equivalent for the two grants most likely to "fight" the
# provider, plus the contingency grants and the teardown of any CLI-created LF objects. Use a
# block here ONLY if the matching Terraform resource errors during apply; record what you ran
# in PROGRESS.md so teardown stays accurate. Everything else stays in Terraform.
# ============================================================================
set -uo pipefail
REGION="${AWS_REGION:-eu-west-2}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
DB="${GLUE_DATABASE:-gov_spend_assurance}"
TABLE="${GOVERNED_TABLE:-silver_clean_spend}"
PROJECT="${PROJECT:-govspend}"
DEPT="${ROW_FILTER_DEPARTMENT:-mod}"
FILTER_NAME="region-analyst-${DEPT}-rows"
REGION_ROLE="arn:aws:iam::${ACCOUNT}:role/${PROJECT}-region-analyst-role"

case "${1:-help}" in

  # Fallback for aws_lakeformation_data_cells_filter (run after the Glue job creates the table).
  create-filter)
    aws lakeformation create-data-cells-filter --region "$REGION" --table-data "{
      \"TableCatalogId\": \"${ACCOUNT}\",
      \"DatabaseName\": \"${DB}\",
      \"TableName\": \"${TABLE}\",
      \"Name\": \"${FILTER_NAME}\",
      \"RowFilter\": {\"FilterExpression\": \"source_department = '${DEPT}'\"},
      \"ColumnWildcard\": {}
    }"
    ;;

  # Fallback for aws_lakeformation_permissions.region_analyst_filter (grant the filter, SELECT).
  grant-filter)
    aws lakeformation grant-permissions --region "$REGION" \
      --principal "DataLakePrincipalIdentifier=${REGION_ROLE}" \
      --permissions SELECT \
      --resource "{\"DataCellsFilter\":{\"TableCatalogId\":\"${ACCOUNT}\",\"DatabaseName\":\"${DB}\",\"TableName\":\"${TABLE}\",\"Name\":\"${FILTER_NAME}\"}}"
    ;;

  # Contingency: some Athena+LF setups need DESCRIBE on the 'default' database for each consumer.
  grant-default-describe)
    for r in analyst-role assurance-role region-analyst-role; do
      aws lakeformation grant-permissions --region "$REGION" \
        --principal "DataLakePrincipalIdentifier=arn:aws:iam::${ACCOUNT}:role/${PROJECT}-${r}" \
        --permissions DESCRIBE \
        --resource '{"Database":{"Name":"default"}}'
    done
    ;;

  # Teardown of CLI-created LF objects (Terraform-managed ones go via terraform destroy).
  teardown)
    aws lakeformation revoke-permissions --region "$REGION" \
      --principal "DataLakePrincipalIdentifier=${REGION_ROLE}" \
      --permissions SELECT \
      --resource "{\"DataCellsFilter\":{\"TableCatalogId\":\"${ACCOUNT}\",\"DatabaseName\":\"${DB}\",\"TableName\":\"${TABLE}\",\"Name\":\"${FILTER_NAME}\"}}" || true
    aws lakeformation delete-data-cells-filter --region "$REGION" \
      --table-catalog-id "${ACCOUNT}" --database-name "${DB}" --table-name "${TABLE}" --name "${FILTER_NAME}" || true
    ;;

  *)
    echo "usage: $0 {create-filter|grant-filter|grant-default-describe|teardown}"
    echo "  Only use a block if the matching Terraform resource fails. Record it in PROGRESS.md."
    ;;
esac
