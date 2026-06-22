import os, sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.appName("tests").master("local[2]")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
