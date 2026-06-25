# ============================================================================
# IAM — one Glue writer role + three consumer roles for the access-control proof.
#
# Design split (forced by how Lake Formation works):
#   - Glue writer: Full Table Access (FGAC cannot write on Glue 5.0). Reads raw/scripts
#     via its own S3 IAM perms; writes the LF-registered curated location via LF credential
#     vending (lakeformation:GetDataAccess), NOT direct S3 — so curated is deliberately
#     absent from its S3 statements.
#   - analyst / assurance: governed by LF-Tags (column governance). Identical IAM; the
#     DIFFERENCE between them is the LF-Tag grant in lakeformation.tf, nothing here.
#   - region-analyst: governed by a Lake Formation data filter (row/cell). Same IAM plus
#     the extra cell-level query-planning actions Athena needs for a filtered read.
# ============================================================================

# --------------------------------------------------------------- Glue role -- #
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.project}-glue-etl-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

data "aws_iam_policy_document" "glue_policy" {
  # Glue Data Catalog: read + create/update/delete the Iceberg tables and their partitions.
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
      "glue:GetPartition", "glue:GetPartitions", "glue:BatchGetPartition",
      "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
      "glue:CreatePartition", "glue:BatchCreatePartition", "glue:UpdatePartition",
      "glue:BatchUpdatePartition", "glue:DeletePartition",
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${local.account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${local.account_id}:database/${var.glue_database}",
      "arn:aws:glue:${var.aws_region}:${local.account_id}:table/${var.glue_database}/*",
    ]
  }

  # Read the raw spend/reference CSVs and the scripts/config; the raw bucket is NOT
  # LF-registered, so it is read with the role's own credentials.
  statement {
    sid     = "S3ReadRawAndScripts"
    actions = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.this["raw"].arn, "${aws_s3_bucket.this["raw"].arn}/*",
      aws_s3_bucket.this["scripts"].arn, "${aws_s3_bucket.this["scripts"].arn}/*",
    ]
  }

  # Spark temp / shuffle staging lives under the scripts bucket (--TempDir).
  statement {
    sid       = "S3WriteScriptsTemp"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.this["scripts"].arn}/*"]
  }

  # Write the Iceberg data + metadata to the curated warehouse. Iceberg's S3FileIO writes
  # with the task role's OWN credentials (Lake Formation credential vending does not cover the
  # FileIO write path), so the writer needs direct S3 here. LF registration of curated does not
  # add an S3 deny, so this IAM write path coexists with LF read-governance: readers have NO
  # direct S3 on curated and are governed by LF at query time.
  statement {
    sid     = "S3CuratedReadWrite"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.this["curated"].arn, "${aws_s3_bucket.this["curated"].arn}/*",
    ]
  }

  # CloudWatch Logs for the job.
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:/aws-glue/*"]
  }

  # Custom metric (valid/rejected counts). PutMetricData cannot be resource-scoped, so it is
  # constrained to our namespace via the only condition key it supports.
  statement {
    sid       = "CloudWatchMetric"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["GovSpendETL"]
    }
  }

  # Lake Formation credential vending for the LF-registered curated location (FTA writes).
  statement {
    sid       = "LakeFormationDataAccess"
    actions   = ["lakeformation:GetDataAccess"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "glue" {
  name   = "${var.project}-glue-etl-policy"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_policy.json
}

# ------------------------------------------------------- consumer roles ----- #
# Trust: only the operator IAM user (var.operator_user_name) may assume these, to run the proof queries.
data "aws_iam_policy_document" "consumer_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [data.aws_caller_identity.current.arn]
    }
  }
}

# Shared consumer permissions: run Athena queries in our workgroup, read catalog metadata,
# let Lake Formation vend data credentials, and read/write the (IAM-governed) results bucket.
# Lake Formation does NOT govern the results bucket, so it is locked down here by IAM only.
data "aws_iam_policy_document" "consumer_base" {
  statement {
    sid = "AthenaQuery"
    actions = [
      "athena:StartQueryExecution", "athena:StopQueryExecution",
      "athena:GetQueryExecution", "athena:GetQueryResults", "athena:GetWorkGroup",
      "athena:ListQueryExecutions", "athena:BatchGetQueryExecution",
    ]
    resources = ["arn:aws:athena:${var.aws_region}:${local.account_id}:workgroup/${aws_athena_workgroup.this.name}"]
  }

  statement {
    sid       = "AthenaList"
    actions   = ["athena:ListEngineVersions", "athena:ListWorkGroups", "athena:GetDataCatalog"]
    resources = ["*"]
  }

  statement {
    sid = "GlueCatalogRead"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
      "glue:GetPartition", "glue:GetPartitions",
    ]
    resources = [
      "arn:aws:glue:${var.aws_region}:${local.account_id}:catalog",
      "arn:aws:glue:${var.aws_region}:${local.account_id}:database/${var.glue_database}",
      "arn:aws:glue:${var.aws_region}:${local.account_id}:table/${var.glue_database}/*",
    ]
  }

  statement {
    sid       = "LakeFormationDataAccess"
    actions   = ["lakeformation:GetDataAccess"]
    resources = ["*"]
  }

  statement {
    sid     = "AthenaResultsBucket"
    actions = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = [
      aws_s3_bucket.this["athena_results"].arn,
      "${aws_s3_bucket.this["athena_results"].arn}/*",
    ]
  }
}

# Extra actions a cell/row-filtered read needs (Lake Formation storage-API query path).
data "aws_iam_policy_document" "consumer_cell_level" {
  source_policy_documents = [data.aws_iam_policy_document.consumer_base.json]
  statement {
    sid = "LakeFormationQueryPlanning"
    actions = [
      "lakeformation:StartQueryPlanning", "lakeformation:GetQueryState",
      "lakeformation:GetWorkUnits", "lakeformation:GetWorkUnitResults",
    ]
    resources = ["*"]
  }
}

# analyst-role — LF-Tag column governance: classification IN (public, internal).
resource "aws_iam_role" "analyst" {
  name               = "${var.project}-analyst-role"
  assume_role_policy = data.aws_iam_policy_document.consumer_assume.json
}
resource "aws_iam_role_policy" "analyst" {
  name   = "athena-query"
  role   = aws_iam_role.analyst.id
  policy = data.aws_iam_policy_document.consumer_base.json
}

# assurance-role — LF-Tag column governance: classification IN (public, internal, restricted).
resource "aws_iam_role" "assurance" {
  name               = "${var.project}-assurance-role"
  assume_role_policy = data.aws_iam_policy_document.consumer_assume.json
}
resource "aws_iam_role_policy" "assurance" {
  name   = "athena-query"
  role   = aws_iam_role.assurance.id
  policy = data.aws_iam_policy_document.consumer_base.json
}

# region-analyst-role — Lake Formation data filter (row/cell). Kept SEPARATE from the
# LF-Tag principals because a tag grant and a filter grant on one principal UNION (widen)
# and defeat the filter.
resource "aws_iam_role" "region_analyst" {
  name               = "${var.project}-region-analyst-role"
  assume_role_policy = data.aws_iam_policy_document.consumer_assume.json
}
resource "aws_iam_role_policy" "region_analyst" {
  name   = "athena-query"
  role   = aws_iam_role.region_analyst.id
  policy = data.aws_iam_policy_document.consumer_cell_level.json
}

# Let the operator (var.operator_user_name) assume the three demo roles. Additive inline policy on the
# existing user; removed on destroy. Toggle off if the user is managed elsewhere.
resource "aws_iam_user_policy" "operator_assume" {
  count = var.manage_operator_assume_policy ? 1 : 0
  name  = "${var.project}-assume-demo-roles"
  user  = local.operator_user_name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRole"
      Resource = [
        aws_iam_role.analyst.arn,
        aws_iam_role.assurance.arn,
        aws_iam_role.region_analyst.arn,
      ]
    }]
  })
}
