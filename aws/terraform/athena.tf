resource "aws_athena_workgroup" "this" {
  name          = "${var.project}-workgroup"
  state         = "ENABLED"
  force_destroy = true # allow destroy even though it holds the named proof queries

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    # Cell-level (data filter) security works in all regions ONLY on engine v3 — exact literal.
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.this["athena_results"].id}/results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# The four access-control proof queries, stored in the workgroup so they are reproducible.
# They are run by ASSUMING each role (see RUNBOOK / athena/run_proof.sh), not from here.

# 1) analyst: select only permitted (public/internal) columns -> SUCCEEDS.
resource "aws_athena_named_query" "analyst_allowed" {
  name      = "01_analyst_allowed_columns"
  workgroup = aws_athena_workgroup.this.id
  database  = aws_glue_catalog_database.this.name
  query     = <<-SQL
    -- Run as analyst-role. LF-Tag grant = classification IN (public, internal).
    -- Restricted columns are simply absent for this principal; this returns fine.
    SELECT source_department, payment_month, supplier_name, amount_gbp, vat_amount_gbp,
           expense_category, supplier_category, cost_centre, processing_status, needs_review
    FROM ${var.glue_database}.${local.governed_table}
    ORDER BY payment_month
    LIMIT 50;
  SQL
}

# 2) analyst: explicitly name a RESTRICTED column -> ACCESS DENIED (the column-governance proof).
resource "aws_athena_named_query" "analyst_denied" {
  name      = "02_analyst_denied_restricted_column"
  workgroup = aws_athena_workgroup.this.id
  database  = aws_glue_catalog_database.this.name
  query     = <<-SQL
    -- Run as analyst-role. Naming `approver` (classification=restricted) must be DENIED.
    -- SELECT * would silently drop it; naming it explicitly is the clear, unambiguous proof.
    SELECT transaction_id, approver, risk_rating
    FROM ${var.glue_database}.${local.governed_table}
    LIMIT 50;
  SQL
}

# 3) assurance: select the same restricted columns -> SUCCEEDS, and sees ALL rows.
resource "aws_athena_named_query" "assurance_allowed" {
  name      = "03_assurance_allowed_restricted_columns"
  workgroup = aws_athena_workgroup.this.id
  database  = aws_glue_catalog_database.this.name
  query     = <<-SQL
    -- Run as assurance-role. LF-Tag grant = classification IN (public, internal, restricted).
    SELECT transaction_id, supplier_name, approver, budget_owner, risk_rating,
           internal_notes, due_diligence_notes
    FROM ${var.glue_database}.${local.governed_table}
    LIMIT 50;
  SQL
}

# 4) region-analyst: data filter restricts rows -> returns ONLY the permitted department.
resource "aws_athena_named_query" "region_analyst_rows" {
  name      = "04_region_analyst_row_filter"
  workgroup = aws_athena_workgroup.this.id
  database  = aws_glue_catalog_database.this.name
  query     = <<-SQL
    -- Run as region-analyst-role. The Lake Formation data filter limits rows to one
    -- department; this aggregate should show ONLY '${var.row_filter_department}'.
    SELECT source_department, count(*) AS rows_visible, sum(amount_gbp) AS total_spend
    FROM ${var.glue_database}.${local.governed_table}
    GROUP BY source_department
    ORDER BY source_department;
  SQL
}
