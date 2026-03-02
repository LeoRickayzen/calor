from __future__ import annotations

import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from etl.job.etl import _aggregate_by_primary_key_year_bought_year_sold, _build_heatmap_result


def test_aggregate_by_primary_key_year_bought_year_sold_mixed_appreciation(spark: SparkSession) -> None:
    schema = StructType([
        StructField("primary_key", StringType()),
        StructField("_year_bought", IntegerType()),
        StructField("_year_sold", IntegerType()),
        StructField("_appreciation_pounds", DoubleType()),
        StructField("_appreciation_pct", DoubleType()),
    ])
    rows = [
        ("county#hackney#flat#freehold#all#all", 2019, 2021, 10.0, 2.0),
        ("county#hackney#flat#freehold#all#all", 2019, 2021, -5.0, -1.0),
        ("county#hackney#flat#freehold#all#all", 2019, 2021, 0.0, 0.0),
    ]
    df = spark.createDataFrame(rows, schema)
    result = _aggregate_by_primary_key_year_bought_year_sold(df)
    rows_out = result.collect()
    assert len(rows_out) == 1
    r = rows_out[0]
    assert r["sale_count"] == 3
    assert r["avg_appreciation_pounds"] == (10.0 - 5.0 + 0.0) / 3.0
    assert r["avg_appreciation_pct"] == (2.0 - 1.0 + 0.0) / 3.0
    assert r["biggest_loser_pounds"] == -5.0
    assert r["biggest_loser_pct"] == -1.0
    assert r["biggest_winner_pounds"] == 10.0
    assert r["biggest_winner_pct"] == 2.0
    assert r["pct_sales_appreciated"] == pytest.approx(100.0 / 3.0, rel=1e-5)


def test_aggregate_by_primary_key_year_bought_year_sold_all_non_positive(spark: SparkSession) -> None:
    schema = StructType([
        StructField("primary_key", StringType()),
        StructField("_year_bought", IntegerType()),
        StructField("_year_sold", IntegerType()),
        StructField("_appreciation_pounds", DoubleType()),
        StructField("_appreciation_pct", DoubleType()),
    ])
    rows = [
        ("county#hackney#flat#freehold#all#all", 2019, 2021, -1.0, -0.5),
        ("county#hackney#flat#freehold#all#all", 2019, 2021, 0.0, 0.0),
    ]
    df = spark.createDataFrame(rows, schema)
    result = _aggregate_by_primary_key_year_bought_year_sold(df)
    rows_out = result.collect()
    assert len(rows_out) == 1
    assert rows_out[0]["pct_sales_appreciated"] == 0.0


def test_build_heatmap_result_per_key_grouping_and_sort_order(spark: SparkSession) -> None:
    schema = StructType([
        StructField("primary_key", StringType()),
        StructField("_year_bought", IntegerType()),
        StructField("_year_sold", IntegerType()),
        StructField("avg_appreciation_pounds", DoubleType()),
        StructField("median_appreciation_pounds", DoubleType()),
        StructField("sale_count", IntegerType()),
        StructField("avg_appreciation_pct", DoubleType()),
        StructField("median_appreciation_pct", DoubleType()),
        StructField("pct_sales_appreciated", DoubleType()),
        StructField("biggest_loser_pounds", DoubleType()),
        StructField("biggest_loser_pct", DoubleType()),
        StructField("biggest_winner_pounds", DoubleType()),
        StructField("biggest_winner_pct", DoubleType()),
    ])
    rows = [
        ("county#hackney#flat#freehold#all#all", 2021, 2023, 50.0, 50.0, 1, 5.0, 5.0, 100.0, 50.0, 5.0, 50.0, 5.0),
        ("county#hackney#flat#freehold#all#all", 2019, 2021, 30.0, 30.0, 1, 3.0, 3.0, 100.0, 30.0, 3.0, 30.0, 3.0),
    ]
    df = spark.createDataFrame(rows, schema)
    result = _build_heatmap_result(df)
    rows_out = result.collect()
    assert len(rows_out) == 1
    row = rows_out[0]
    heatmap_graph = json.loads(row["heatmap_graph"])
    assert len(heatmap_graph) == 2
    assert heatmap_graph[0]["year_bought"] == "2019"
    assert heatmap_graph[0]["year_sold"] == "2021"
    assert heatmap_graph[1]["year_bought"] == "2021"
    assert heatmap_graph[1]["year_sold"] == "2023"
