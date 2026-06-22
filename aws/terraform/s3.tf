# Four buckets: raw landing, curated lakehouse (Iceberg warehouse + LF-registered),
# scripts (Glue job + shared src.zip), and Athena query results.

resource "aws_s3_bucket" "this" {
  for_each = local.buckets
  bucket   = each.value
}

# Block ALL public access on every bucket (defence in depth for sensitive procurement data).
resource "aws_s3_bucket_public_access_block" "this" {
  for_each = aws_s3_bucket.this

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# SSE-S3 (AES256) at rest on every bucket. No KMS to keep the demo free of KMS charges.
resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Versioning on the data + code buckets so an overwrite is recoverable; not on results (disposable).
resource "aws_s3_bucket_versioning" "data" {
  for_each = toset(["raw", "curated", "scripts"])

  bucket = aws_s3_bucket.this[each.key].id
  versioning_configuration {
    status = "Enabled"
  }
}

# Athena results are disposable: expire them so the bucket never grows unbounded (cost control).
resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.this["athena_results"].id

  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter {}
    expiration {
      days = var.athena_results_expiry_days
    }
  }
}
