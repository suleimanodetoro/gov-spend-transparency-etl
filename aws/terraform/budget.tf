# AWS Budgets — the FIRST billable resource, applied before anything else and only with
# approval. Apply it on its own first:  terraform apply -target=aws_budgets_budget.monthly
# It guards the whole demo: an email fires at 80% actual and 100% forecast spend.
#
# NOTE: limit_unit must match the account's billing currency. AWS bills in USD unless the
# account's currency preference was changed; override var.budget_currency if yours differs.
resource "aws_budgets_budget" "monthly" {
  name         = "${var.project}-monthly-budget"
  budget_type  = "COST"
  limit_amount = var.budget_limit_amount
  limit_unit   = var.budget_currency
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_notification_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_notification_email]
  }
}
