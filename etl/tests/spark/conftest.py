from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("etl-spark-tests")
        .master("local[1]")
        .getOrCreate()
    )
    yield session
    session.stop()
