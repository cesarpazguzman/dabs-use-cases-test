import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Make src/ importable, as it is for the notebook in Databricks
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(scope="session")
def spark():
    """Small local Spark session: no Databricks workspace or credentials needed."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("sales-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
