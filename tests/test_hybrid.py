import transforms as T


def test_attach_assurance_adds_approval_and_supplier_fields(spark):
    silver = spark.createDataFrame([("T1", "Acme", 100.0)], ["transaction_id", "supplier_name", "amount_gbp"])
    approvals = spark.createDataFrame(
        [("T1", "CC1", "a.x", "Owner", "Approved", "note")],
        ["transaction_id", "cost_centre", "approver", "budget_owner", "processing_status", "internal_notes"])
    suppliers = spark.createDataFrame(
        [("Acme", "Strategic", "High", "dd")],
        ["supplier_name", "supplier_category", "risk_rating", "due_diligence_notes"])
    out = T.attach_assurance(spark, silver, approvals, suppliers).collect()[0]
    assert out["approver"] == "a.x"
    assert out["risk_rating"] == "High"
    assert out["missing_approval"] is False


def test_open_layer_removes_restricted_columns(spark):
    df = spark.createDataFrame([("T1", "a.x", "High", 100.0)],
                               ["transaction_id", "approver", "risk_rating", "amount_gbp"])
    cols = T.open_layer(df, ["approver", "risk_rating"]).columns
    assert "approver" not in cols and "risk_rating" not in cols
    assert "amount_gbp" in cols and "transaction_id" in cols
