import transforms as T
from pyspark.sql import functions as F


def test_parse_amount_handles_currency_commas_and_credits(spark):
    df = spark.createDataFrame([("\u00A31,234.56",), ("(500.00)",), ("1,250",), ("INVALID",), ("",)], ["amount"])
    got = [r[0] for r in df.select(T.parse_amount("amount").alias("a")).collect()]
    assert got == [1234.56, -500.0, 1250.0, None, None]


def test_parse_date_tries_formats_and_rejects_invalid(spark):
    df = spark.createDataFrame([("01/05/2026",), ("2026-05-01",), ("01-May-2026",), ("31/02/2026",), ("nope",)], ["d"])
    got = [str(r[0]) if r[0] is not None else None for r in df.select(T.parse_date("d").alias("x")).collect()]
    assert got == ["2026-05-01", "2026-05-01", "2026-05-01", None, None]


def test_normalise_maps_known_and_captures_unknown_to_raw_extras(spark):
    df = (spark.createDataFrame([("R1", "ACME", "01/05/2026", "100", "Cat", "X")],
                                ["Ref", "Vendor", "Payment Date", "Gross (\u00A3)", "Cost Centre", "Mystery"])
          .withColumn("source_file", F.lit("f.csv")))
    mapping = {"Ref": "transaction_id", "Vendor": "supplier_name", "Payment Date": "payment_date",
               "Gross (\u00A3)": "amount", "Cost Centre": "expense_category"}
    out = T.normalise(df, mapping, "mod", "v1").collect()[0]
    assert out["transaction_id"] == "R1"
    assert out["raw_extras"] == {"Mystery": "X"}
    assert out["source_department"] == "mod"


def test_record_hash_is_64_hex(spark):
    df = spark.createDataFrame([("T1", "mod", "2026-05-01", 10.0)],
                               ["transaction_id", "source_department", "payment_date", "amount_gbp"])
    h = T.add_record_hash(df).collect()[0]["record_hash"]
    assert isinstance(h, str) and len(h) == 64
