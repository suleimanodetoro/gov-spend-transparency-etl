# TEARDOWN — leave nothing billable running

Run in order when the demo/captures are done. Goal: zero lingering billable resources.

> `export AWS_REGION=eu-west-2 AWS_DEFAULT_REGION=eu-west-2`

## 0. Pre-checks (what is up)
```bash
aws glue get-jobs --query 'Jobs[].Name' --region eu-west-2
aws glue get-databases --query 'DatabaseList[].Name' --region eu-west-2
aws s3 ls | grep govspend
aws athena list-work-groups --region eu-west-2 --query 'WorkGroups[].Name'
```

## 1. Any LF objects created via the CLI fallback (only if you used lakeformation_cli.sh)
```bash
cd aws && ./lakeformation_cli.sh teardown && cd ..
```

## 2. Empty S3 buckets (Terraform cannot destroy non-empty buckets)
```bash
for b in $(aws s3 ls | awk '{print $3}' | grep govspend); do
  echo "emptying s3://$b"; aws s3 rm "s3://$b" --recursive
done
```

## 3. Terraform destroy (S3, Glue + Iceberg tables, IAM, LF stage-1+2, Athena, Budget)
```bash
cd aws/terraform
# destroy must know about the stage-2 resources, so pass the same var used to create them:
terraform destroy -var enable_table_governance=true
```
Notes:
- The Glue job created the Iceberg tables OUTSIDE Terraform (Terraform never knew them). Dropping
  the Glue database via `terraform destroy` removes the catalog entries; the data is deleted by the
  bucket-empty in step 2. If `destroy` complains the database is not empty, drop the tables first:
  `for t in silver_clean_spend curated_open_analytics gold_department_metrics rejected_records; do aws glue delete-table --database-name gov_spend_assurance --name $t --region eu-west-2; done`
- LF revoke/destroy is occasionally buggy (provider issues #35160/#36827). If a permission won't
  revoke, remove it in the LF console (Permissions / Data filters / LF-Tags) and re-run destroy.

## 4. Reset Lake Formation account settings if desired (optional)
`terraform destroy` removes the `aws_lakeformation_data_lake_settings` resource, which restores the
default. If you want to re-add the `IAMAllowedPrincipals` Super default explicitly, do it in the LF
console under Administration → Data lake settings.

## 4b. Gotchas actually hit on the first teardown (2026-06-21) — do these to avoid them
- **Deleting Glue tables manually first orphans LF state.** The column LF-tag + permission
  resources reference the tables; once the tables are gone, `terraform destroy`'s refresh fails
  with `EntityNotFound`. Remove them from state before destroy:
  ```bash
  terraform state rm 'aws_lakeformation_resource_lf_tags.columns_public[0]' \
    'aws_lakeformation_resource_lf_tags.columns_internal[0]' \
    'aws_lakeformation_resource_lf_tags.columns_restricted[0]'
  ```
  (Use single quotes — in zsh, `"$r[0]"` is parsed as an array subscript and silently no-ops.)
- **Even an LF admin needs an explicit DROP grant** to delete a governed table or an LF-tag:
  ```bash
  aws lakeformation grant-permissions --principal DataLakePrincipalIdentifier=<you> \
    --permissions DROP DESCRIBE --resource '{"Table":{"DatabaseName":"gov_spend_assurance","TableWildcard":{}}}'
  ```
- **Versioned buckets** keep old versions + delete-markers after `s3 rm`; purge all versions
  (see purge loop used in PROGRESS) before `aws s3 rb`.
- **Deregistering the LAST LF S3 location** can demand the service-linked role be deleted; if it
  blocks, `terraform state rm aws_lakeformation_resource.curated`, delete the bucket, and the
  registration is harmlessly orphaned (free) — or delete `AWSServiceRoleForLakeFormationDataAccess`.

## 5. Final verification — expect empty / not-found
```bash
aws s3 ls | grep govspend || echo "no govspend buckets — OK"
aws glue get-databases --query 'DatabaseList[].Name' --region eu-west-2
aws athena list-work-groups --region eu-west-2 --query 'WorkGroups[].Name'
aws budgets describe-budgets --account-id "$(aws sts get-caller-identity --query Account --output text)" --query 'Budgets[].BudgetName' 2>/dev/null || true
```
