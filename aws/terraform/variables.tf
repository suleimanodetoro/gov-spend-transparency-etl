variable "aws_region" {
  description = "AWS region for all resources. London by default; never us-east-1 for this project."
  type        = string
  default     = "eu-west-2"
}

variable "project" {
  description = "Short name used as a prefix for resources and the bucket family."
  type        = string
  default     = "govspend"
}

variable "glue_database" {
  description = "Glue Data Catalog database for the governed lakehouse."
  type        = string
  default     = "gov_spend_assurance"
}

variable "glue_job_name" {
  description = "Name of the Glue ETL job."
  type        = string
  default     = "gov-spend-assurance-etl"
}

variable "glue_version" {
  description = "AWS Glue runtime version. 5.0 = Spark 3.5 / Python 3.11 / Java 17."
  type        = string
  default     = "5.0"
}

variable "glue_worker_type" {
  description = "Glue worker size. G.1X is ample for the sample volume and is the cheapest DPU class."
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Worker count. 2 (the minimum) keeps the demo cheap; the job scales by raising this."
  type        = number
  default     = 2
}

# --- AWS Budgets (the first billable thing created, only after approval) ---
variable "budget_limit_amount" {
  description = "Monthly cost budget threshold."
  type        = string
  default     = "10"
}

variable "budget_currency" {
  description = "Budget currency. MUST match the account's billing currency (AWS bills in USD unless changed)."
  type        = string
  default     = "USD"
}

variable "budget_notification_email" {
  description = "Email that receives budget alerts. Set to your own address."
  type        = string
  default     = "alerts@example.com"
}

variable "athena_results_expiry_days" {
  description = "Lifecycle expiry for the Athena results bucket (results are disposable)."
  type        = number
  default     = 7
}

variable "enable_table_governance" {
  description = <<-EOT
    Two-stage apply switch. The column LF-Tag assignments, the data cells filter, and the
    consumer grants all reference the Iceberg TABLES, which do not exist until the Glue job
    has run. Apply once with this false (creates infra + runs the job), then apply again with
    this true to layer on the table-level Lake Formation governance.
  EOT
  type        = bool
  default     = false
}

variable "operator_user_name" {
  description = "Existing IAM user (in your account) that runs Terraform and assumes the demo roles. No default — supply via TF_VAR_operator_user_name (see aws/RUNBOOK.md)."
  type        = string
}

variable "manage_operator_assume_policy" {
  description = "Attach an additive inline policy letting operator_user_name assume the 3 demo roles. Off if the user is managed elsewhere."
  type        = bool
  default     = true
}

variable "row_filter_department" {
  description = "The single department region-analyst-role may see, proving the Lake Formation row filter."
  type        = string
  default     = "mod"
}

variable "manage_data_filter_in_tf" {
  description = <<-EOT
    Whether Terraform manages the data cells filter + its grant. The aws_lakeformation_data_cells_filter
    resource intermittently trips "Provider produced inconsistent result after apply" (provider bug),
    so by default these two are managed via aws/lakeformation_cli.sh instead (the brief's documented
    CLI fallback). The filter is still created in AWS; only the management plane differs.
  EOT
  type        = bool
  default     = false
}
