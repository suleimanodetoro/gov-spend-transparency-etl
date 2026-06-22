# ============================================================================
# Lake Formation — the governance layer over the ONE Iceberg table.
#
# TWO complementary mechanisms, kept on SEPARATE principals on purpose:
#   - LF-Tags (tag-based / ABAC)  -> COLUMN governance for analyst-role & assurance-role.
#   - Data cells filter            -> ROW/cell governance for region-analyst-role.
# They are not combined on one principal because LF UNIONS a tag grant with a named-resource
# grant (a data-filter grant IS a named-resource grant). The union WIDENS access, so a tag
# grant placed next to a filter on the same principal silently defeats the row filter.
# (Verbatim AWS rule: "...the permissions that the principal has on the resource is the union
# of the permissions granted by both methods.")
#
# Staging: column tag ASSIGNMENTS, the data filter, and the consumer GRANTS reference the
# Iceberg tables, which the Glue job creates at run time. They are gated behind
# var.enable_table_governance so stage 1 stands up the account/db/role, then stage 2 (after
# the job) layers the table-level governance. Permission lists are kept lexically sorted to
# avoid the provider's plan-churn bug (#31096).
# ============================================================================

# --- Stage 1: account-level LF setup (no table dependency) ------------------ #

# WARNING: data_lake_settings is ACCOUNT-WIDE and authoritative. Omitting create_*_default
# blocks clears the IAMAllowedPrincipals "Super" default so LF actually enforces on NEW
# tables. If this account uses Lake Formation elsewhere, this overwrites its admins — only
# apply in a sandbox account.
resource "aws_lakeformation_data_lake_settings" "this" {
  admins = [data.aws_caller_identity.current.arn]

  # Required for Full Table Access credential vending to external engines (the Glue writer).
  allow_full_table_external_data_access = true

  # NOTE: create_database_default_permissions / create_table_default_permissions are
  # intentionally OMITTED -> cleared -> LF enforcement is ON for new tables.
}

# Register the curated warehouse location so LF vends S3 credentials to READERS (Athena
# consumers, who have no direct S3 on curated, are governed by LF at query time). The
# service-linked role handles LF's underlying S3 access to the registered prefix.
#
# The Glue writer writes Iceberg data/metadata here with its OWN IAM role: Iceberg S3FileIO
# uses the task role's credentials for the FileIO write path (not LF-vended creds), and LF
# registration does not add an S3 deny, so the writer's direct IAM S3 access (granted in iam.tf)
# coexists with LF read-governance.
resource "aws_lakeformation_resource" "curated" {
  arn                     = aws_s3_bucket.this["curated"].arn
  use_service_linked_role = true
}

# The ABAC tag. Values mirror data_classification.json.
resource "aws_lakeformation_lf_tag" "classification" {
  key    = "classification"
  values = ["internal", "public", "restricted"]

  depends_on = [aws_lakeformation_data_lake_settings.this]
}

# Baseline: tag the database "internal" so the table inherits it (a tag-policy TABLE grant
# only matches tables that carry a matching tag) and untagged metadata columns default to
# internal rather than public.
resource "aws_lakeformation_resource_lf_tags" "database" {
  database {
    name = aws_glue_catalog_database.this.name
  }
  lf_tag {
    key   = aws_lakeformation_lf_tag.classification.key
    value = "internal"
  }
}

# Glue writer (Full Table Access) needs these as an LF principal once IAMAllowedPrincipals is
# removed: create/alter/drop tables in the db, write the registered location, full table DML.
resource "aws_lakeformation_permissions" "glue_database" {
  principal   = aws_iam_role.glue.arn
  permissions = ["ALTER", "CREATE_TABLE", "DESCRIBE", "DROP"]
  database {
    name = aws_glue_catalog_database.this.name
  }
}

resource "aws_lakeformation_permissions" "glue_data_location" {
  principal   = aws_iam_role.glue.arn
  permissions = ["DATA_LOCATION_ACCESS"]
  data_location {
    arn = aws_lakeformation_resource.curated.arn
  }
}

resource "aws_lakeformation_permissions" "glue_tables" {
  principal   = aws_iam_role.glue.arn
  permissions = ["ALL"]
  table {
    database_name = aws_glue_catalog_database.this.name
    wildcard      = true
  }
}

# --- Stage 2: table-level governance (needs the tables; gated) -------------- #

# Assign classification to columns per data_classification.json. One assignment per level so
# each tag value tags exactly its columns; the metadata columns not in the JSON inherit the
# database's "internal" baseline.
resource "aws_lakeformation_resource_lf_tags" "columns_public" {
  count = var.enable_table_governance ? 1 : 0
  table_with_columns {
    database_name = aws_glue_catalog_database.this.name
    name          = local.governed_table
    column_names  = local.columns_by_level.public
  }
  lf_tag {
    key   = aws_lakeformation_lf_tag.classification.key
    value = "public"
  }
}

resource "aws_lakeformation_resource_lf_tags" "columns_internal" {
  count = var.enable_table_governance ? 1 : 0
  table_with_columns {
    database_name = aws_glue_catalog_database.this.name
    name          = local.governed_table
    column_names  = local.columns_by_level.internal
  }
  lf_tag {
    key   = aws_lakeformation_lf_tag.classification.key
    value = "internal"
  }
}

resource "aws_lakeformation_resource_lf_tags" "columns_restricted" {
  count = var.enable_table_governance ? 1 : 0
  table_with_columns {
    database_name = aws_glue_catalog_database.this.name
    name          = local.governed_table
    column_names  = local.columns_by_level.restricted
  }
  lf_tag {
    key   = aws_lakeformation_lf_tag.classification.key
    value = "restricted"
  }
}

# COLUMN governance grants (LF-Tags) — analyst sees public+internal; assurance sees all.
resource "aws_lakeformation_permissions" "analyst_db" {
  count       = var.enable_table_governance ? 1 : 0
  principal   = aws_iam_role.analyst.arn
  permissions = ["DESCRIBE"]
  lf_tag_policy {
    resource_type = "DATABASE"
    expression {
      key    = aws_lakeformation_lf_tag.classification.key
      values = ["internal", "public"]
    }
  }
}

resource "aws_lakeformation_permissions" "analyst_table" {
  count       = var.enable_table_governance ? 1 : 0
  principal   = aws_iam_role.analyst.arn
  permissions = ["DESCRIBE", "SELECT"]
  lf_tag_policy {
    resource_type = "TABLE"
    expression {
      key    = aws_lakeformation_lf_tag.classification.key
      values = ["internal", "public"]
    }
  }
}

resource "aws_lakeformation_permissions" "assurance_db" {
  count       = var.enable_table_governance ? 1 : 0
  principal   = aws_iam_role.assurance.arn
  permissions = ["DESCRIBE"]
  lf_tag_policy {
    resource_type = "DATABASE"
    expression {
      key    = aws_lakeformation_lf_tag.classification.key
      values = ["internal", "public", "restricted"]
    }
  }
}

resource "aws_lakeformation_permissions" "assurance_table" {
  count       = var.enable_table_governance ? 1 : 0
  principal   = aws_iam_role.assurance.arn
  permissions = ["DESCRIBE", "SELECT"]
  lf_tag_policy {
    resource_type = "TABLE"
    expression {
      key    = aws_lakeformation_lf_tag.classification.key
      values = ["internal", "public", "restricted"]
    }
  }
}

# ROW/cell governance — region-analyst sees ALL columns but only one department's rows.
# This principal gets the data filter ONLY (plus a harmless DESCRIBE on the db so Athena can
# resolve the table) and NO LF-Tag grant, so the row filter is not unioned away.
#
# Managed via aws/lakeformation_cli.sh by default (var.manage_data_filter_in_tf=false): the
# data_cells_filter provider resource intermittently trips "inconsistent result after apply".
resource "aws_lakeformation_data_cells_filter" "region" {
  count = var.enable_table_governance && var.manage_data_filter_in_tf ? 1 : 0
  table_data {
    database_name    = aws_glue_catalog_database.this.name
    table_name       = local.governed_table
    table_catalog_id = local.account_id
    name             = "region-analyst-${var.row_filter_department}-rows"

    column_wildcard {} # all columns -> a pure ROW-level proof

    row_filter {
      filter_expression = "source_department = '${var.row_filter_department}'"
    }
  }
}

resource "aws_lakeformation_permissions" "region_analyst_db" {
  count       = var.enable_table_governance ? 1 : 0
  principal   = aws_iam_role.region_analyst.arn
  permissions = ["DESCRIBE"]
  database {
    name = aws_glue_catalog_database.this.name
  }
}

resource "aws_lakeformation_permissions" "region_analyst_filter" {
  count       = var.enable_table_governance && var.manage_data_filter_in_tf ? 1 : 0
  principal   = aws_iam_role.region_analyst.arn
  permissions = ["SELECT"]
  data_cells_filter {
    database_name    = aws_glue_catalog_database.this.name
    table_name       = local.governed_table
    table_catalog_id = local.account_id
    name             = aws_lakeformation_data_cells_filter.region[0].table_data[0].name
  }
}
