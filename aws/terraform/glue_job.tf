resource "aws_glue_job" "etl" {
  name              = var.glue_job_name
  role_arn          = aws_iam_role.glue.arn
  glue_version      = var.glue_version # 5.0 = Spark 3.5.4 / Python 3.11 / Java 17 / Iceberg 1.7.1
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 30 # minutes; the sample volume finishes in a couple

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.this["scripts"].id}/${local.glue_script_key}"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    # Load the Iceberg runtime jars (the catalog/extensions config itself is set in SparkConf
    # inside glue_job.py, not chained here, to avoid the --conf join footgun).
    "--datalake-formats" = "iceberg"

    # Ship the SAME transforms.py / quality.py used locally and by the unit tests.
    "--extra-py-files" = "s3://${aws_s3_bucket.this["scripts"].id}/${local.src_zip_key}"

    "--job-language" = "python"
    "--TempDir"      = "s3://${aws_s3_bucket.this["scripts"].id}/tmp/"

    "--enable-metrics"               = "true"
    "--enable-observability-metrics" = "true"

    # Job inputs read via getResolvedOptions (hyphen -> underscore in the script).
    "--raw_bucket"     = aws_s3_bucket.this["raw"].id
    "--curated_bucket" = aws_s3_bucket.this["curated"].id
    "--scripts_bucket" = aws_s3_bucket.this["scripts"].id
    "--database"       = var.glue_database
    "--region"         = var.aws_region
    "--account_id"     = local.account_id
    "--catalog"        = "glue_catalog"
    "--warehouse"      = "s3://${aws_s3_bucket.this["curated"].id}/warehouse/"
    # Direct-write mode: writer uses its IAM role for S3 (curated is registered in LF hybrid
    # mode); readers stay governed by LF at query time. Set "true" only to test FTA vending.
    "--lakeformation_enabled" = "false"
  }

  # The script and shared modules must be in S3 before the job definition references them.
  depends_on = [aws_s3_object.glue_script, aws_s3_object.src_zip]
}
