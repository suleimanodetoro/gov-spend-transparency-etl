provider "aws" {
  region = var.aws_region

  # Stamp every resource so the whole stack is identifiable and easy to find/tear down.
  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
      Purpose   = "gov-spend-assurance-etl-interview-demo"
    }
  }
}
