output "region" {
  value = var.aws_region
}

output "account_id" {
  value = local.account_id
}

output "buckets" {
  description = "The four S3 buckets."
  value       = { for k, b in aws_s3_bucket.this : k => b.id }
}

output "glue_database" {
  value = aws_glue_catalog_database.this.name
}

output "glue_job_name" {
  value = aws_glue_job.etl.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.this.name
}

output "consumer_role_arns" {
  description = "Assume these to run the access-control proof queries."
  value = {
    analyst        = aws_iam_role.analyst.arn
    assurance      = aws_iam_role.assurance.arn
    region_analyst = aws_iam_role.region_analyst.arn
  }
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}

output "run_glue_job_command" {
  description = "Kick off the ETL once stage 1 is applied."
  value       = "aws glue start-job-run --job-name ${aws_glue_job.etl.name} --region ${var.aws_region}"
}
