from __future__ import annotations

import json

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from etl.job.etl import _aggregate_by_primary_key_and_year_sold, _build_line_result


def test_aggregate_by_primary_key_and_year_sold_basic_grouping_and_averages(spark: SparkSession) -> None:
    schema = StructType([
        StructField("primary_key", StringType()),
        StructField("_year_sold", IntegerType()),
        StructField("_price", DoubleType()),
    ])
    rows = [
        ("county#hackney#flat#freehold#all#all", 2019, 100.0),
        ("county#hackney#flat#freehold#all#all", 2019, 200.0),
        ("county#hackney#flat#freehold#all#all", 2020, 300.0),
    ]
    df = spark.createDataFrame(rows, schema)
    result = _aggregate_by_primary_key_and_year_sold(df)
    rows_out = result.collect()
    assert len(rows_out) == 2
    by_year = {r["_year_sold"]: r for r in rows_out}
    r2019 = by_year[2019]
    assert r2019["avg_price"] == 150.0
    assert r2019["sale_count"] == 2
    assert r2019["median_price"] == 150.0
    assert r2019["mode_price"] == 150.0
    r2020 = by_year[2020]
    assert r2020["avg_price"] == 300.0
    assert r2020["sale_count"] == 1
    assert r2020["median_price"] == 300.0
    assert r2020["mode_price"] == 300.0


def test_aggregate_by_primary_key_and_year_sold_separate_segments(spark: SparkSession) -> None:
    schema = StructType([
        StructField("primary_key", StringType()),
        StructField("_year_sold", IntegerType()),
        StructField("_price", DoubleType()),
    ])
    rows = [
        ("county#hackney#flat#freehold#all#all", 2020, 250.0),
        ("borough#islington#flat#freehold#all#all", 2020, 300.0),
    ]
    df = spark.createDataFrame(rows, schema)
    result = _aggregate_by_primary_key_and_year_sold(df)
    rows_out = result.collect()
    assert len(rows_out) == 2
    by_key = {r["primary_key"]: r for r in rows_out}
    assert by_key["county#hackney#flat#freehold#all#all"]["avg_price"] == 250.0
    assert by_key["county#hackney#flat#freehold#all#all"]["sale_count"] == 1
    assert by_key["borough#islington#flat#freehold#all#all"]["avg_price"] == 300.0
    assert by_key["borough#islington#flat#freehold#all#all"]["sale_count"] == 1


def test_build_line_result_ordering_and_sale_count_sum(spark: SparkSession) -> None:
    schema = StructType([
        StructField("primary_key", StringType()),
        StructField("_year_sold", IntegerType()),
        StructField("avg_price", DoubleType()),
        StructField("median_price", DoubleType()),
        StructField("mode_price", DoubleType()),
        StructField("sale_count", IntegerType()),
    ])
    rows = [
        ("county#hackney#flat#freehold#all#all", 2021, 280.0, 280.0, 280.0, 2),
        ("county#hackney#flat#freehold#all#all", 2019, 250.0, 250.0, 250.0, 3),
    ]
    df = spark.createDataFrame(rows, schema)
    result = _build_line_result(df)
    rows_out = result.collect()
    assert len(rows_out) == 1
    row = rows_out[0]
    assert row["sale_count"] == 5  # 2 + 3
    line_graph = json.loads(row["line_graph"])
    assert len(line_graph) == 2
    assert line_graph[0]["year_sold"] == "2019"
    assert line_graph[0]["avg_price"] == 250.0
    assert line_graph[0]["sale_count"] == 3
    assert line_graph[1]["year_sold"] == "2021"
    assert line_graph[1]["avg_price"] == 280.0
    assert line_graph[1]["sale_count"] == 2
