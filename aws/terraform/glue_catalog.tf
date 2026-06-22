# The Glue Data Catalog database that holds the governed Iceberg tables.
# Lake Formation governs access to this database and its tables (see lakeformation.tf).
resource "aws_glue_catalog_database" "this" {
  name        = var.glue_database
  description = "Governed government-spend assurance lakehouse (Iceberg tables, Lake Formation ABAC)."

  # Default warehouse location for tables created in this database.
  location_uri = "s3://${aws_s3_bucket.this["curated"].id}/warehouse/"
}
