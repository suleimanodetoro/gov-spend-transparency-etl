# Ship the shared Python modules and the job script + config + sample data to S3.
# ONE codebase: the Glue job imports the SAME transforms.py / quality.py used locally,
# zipped here and passed to the job via --extra-py-files.

data "archive_file" "src" {
  type        = "zip"
  source_dir  = local.src_dir
  output_path = "${path.module}/src.zip"

  # The job only needs transforms + quality; exclude the local orchestrator and caches
  # so the zip is minimal and its hash is stable.
  excludes = ["__pycache__", "pipeline.py"]
}

# --- scripts bucket: the Glue job script and the shared modules zip ---
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.this["scripts"].id
  key    = local.glue_script_key
  source = local.glue_script
  etag   = filemd5(local.glue_script)
}

resource "aws_s3_object" "src_zip" {
  bucket = aws_s3_bucket.this["scripts"].id
  key    = local.src_zip_key
  source = data.archive_file.src.output_path
  etag   = data.archive_file.src.output_md5
}

resource "aws_s3_object" "config" {
  for_each = fileset(local.config_dir, "*.json")

  bucket = aws_s3_bucket.this["scripts"].id
  key    = "config/${each.value}"
  source = "${local.config_dir}/${each.value}"
  etag   = filemd5("${local.config_dir}/${each.value}")
}

# --- raw bucket: departmental spend CSVs + the sensitive reference data ---
# Requires `make data` to have generated ./data first (the runbook does this before apply).
resource "aws_s3_object" "spend" {
  for_each = fileset(local.data_raw_dir, "*.csv")

  bucket = aws_s3_bucket.this["raw"].id
  key    = "spend/${each.value}"
  source = "${local.data_raw_dir}/${each.value}"
  etag   = filemd5("${local.data_raw_dir}/${each.value}")
}

resource "aws_s3_object" "reference" {
  for_each = fileset(local.data_ref_dir, "*.csv")

  bucket = aws_s3_bucket.this["raw"].id
  key    = "reference/${each.value}"
  source = "${local.data_ref_dir}/${each.value}"
  etag   = filemd5("${local.data_ref_dir}/${each.value}")
}
