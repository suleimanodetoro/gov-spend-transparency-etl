# Terraform + provider version constraints.
# AWS provider >= 5.60 is required for the Lake Formation resources used here
# (aws_lakeformation_data_cells_filter, aws_lakeformation_resource_lf_tag(s),
# tag-policy grants). archive provider zips the shared src/ for --extra-py-files.
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # >= 6.32.0: the aws_lakeformation_data_cells_filter `table_data {}` block schema
      # (column_wildcard / row_filter disambiguation) landed in this provider version.
      version = ">= 6.32.0, < 7.0.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}
