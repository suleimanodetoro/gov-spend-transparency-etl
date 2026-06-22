data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id

  # Buckets are globally unique, so suffix with the account id.
  buckets = {
    raw            = "${var.project}-raw-${local.account_id}"
    curated        = "${var.project}-curated-${local.account_id}"
    scripts        = "${var.project}-scripts-${local.account_id}"
    athena_results = "${var.project}-athena-results-${local.account_id}"
  }

  # Repo paths (Terraform runs from aws/terraform).
  repo_root      = "${path.module}/../.."
  src_dir        = "${local.repo_root}/src"
  config_dir     = "${local.repo_root}/config"
  data_raw_dir   = "${local.repo_root}/data/raw"
  data_ref_dir   = "${local.repo_root}/data/reference"
  glue_script    = "${path.module}/../glue_job.py"
  src_zip_output = "${path.module}/build/src.zip"

  # S3 key layout inside the scripts/raw buckets.
  glue_script_key = "scripts/glue_job.py"
  src_zip_key     = "scripts/src.zip"

  operator_user_name = var.operator_user_name

  # The governed table the access-control proof runs against.
  governed_table = "silver_clean_spend"

  # Single source of truth for column governance: the SAME data_classification.json the local
  # pipeline uses, grouped by level so each LF-Tag value tags exactly its columns.
  classification = jsondecode(file("${local.config_dir}/data_classification.json"))
  columns_by_level = {
    public     = [for c, l in local.classification.columns : c if l == "public"]
    internal   = [for c, l in local.classification.columns : c if l == "internal"]
    restricted = [for c, l in local.classification.columns : c if l == "restricted"]
  }
}
