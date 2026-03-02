#!/usr/bin/env python3
"""
Spark ETL: read Land Registry PPD CSV, add filter/location columns and property_id,
expand to all filter permutations. Line graph = aggregate all sales by (segment, year_sold).
Heatmap = aggregate repeat sales by (segment, year_bought, year_sold). Join and write
(key, value) CSVs for house_price_performance and dimension_index.

Run from repo root: python -m etl.job.etl
Use --hackney to filter the initial dataset to borough Hackney only (district = Hackney).
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from etl.job.constants import EXPANSION_MASKS
from etl.job.etl_helpers import (
    combine_line_and_heatmap_to_value,
    heatmap_graph_json_from_list,
    line_graph_json_from_list,
    normalise,
    normalise_duration,
    normalise_postcode_prefix,
    normalise_property_type,
)
from etl.job.models import HeatmapRow, LineGraphRow

# Paths relative to this file (etl/job/etl.py -> etl/ -> repo root)
ETL_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = ETL_DIR.parent

# PPD CSV column order when file has no header (positional columns).
# Matches Land Registry pp-complete: Transaction ID, Price, Date, Postcode, Property Type, Old/New, Duration, PAON, SAON, Street, Locality, Town, District, County, PPD Category Type, Record Status
PPD_COLUMN_ORDER = [
    "transaction_id",
    "price",
    "date_of_transfer",
    "postcode",
    "property_type",
    "old_new",
    "duration",
    "paon",
    "saon",
    "street",
    "locality",
    "town",
    "district",
    "county",
    "ppd_category_type",
    "record_status",
]


def _read_yaml_config() -> dict[str, Any]:
    conf_path = ETL_DIR / "conf" / "etl_config.yaml"
    if not conf_path.exists():
        return {}
    with open(conf_path) as config_file:
        return yaml.safe_load(config_file) or {}


def _load_config() -> dict[str, str]:
    cfg = _read_yaml_config()
    return {
        "input_path": os.environ.get("ETL_INPUT_PATH") or cfg.get("input_path", "etl/input/ppd"),
        "output_path": os.environ.get("ETL_OUTPUT_PATH") or cfg.get("output_path", "etl/output"),
    }


def _resolve_paths(config: dict[str, str]) -> tuple[Path, Path]:
    input_path = REPO_ROOT / config["input_path"]
    output_path = REPO_ROOT / config["output_path"]
    if not str(input_path).startswith(str(REPO_ROOT)):
        input_path = Path(config["input_path"])
    if not str(output_path).startswith(str(REPO_ROOT)):
        output_path = Path(config["output_path"])
    return input_path, output_path


def _create_spark() -> SparkSession:
    return SparkSession.builder.appName("houses-etl").getOrCreate()


def _read_ppd(spark: SparkSession, input_path: Path) -> DataFrame:
    """Read PPD CSV without header; columns are assigned by position per PPD_COLUMN_ORDER."""
    ppd_path = str(REPO_ROOT / input_path) if not os.path.isabs(str(input_path)) else str(input_path)
    dataframe = spark.read.option("header", "false").option("inferSchema", "false").csv(ppd_path)
    for index, name in enumerate(PPD_COLUMN_ORDER):
        if index < len(dataframe.columns):
            dataframe = dataframe.withColumnRenamed(dataframe.columns[index], name)
    return dataframe


def _price_column(columns: list[str]) -> str:
    return "price" if "price" in columns else [column for column in columns if "price" in column][:1][0]


def _date_column(columns: list[str]) -> str:
    candidate = next((column for column in columns if "date" in column and "transfer" in column), "date_of_transfer")
    return candidate if candidate in columns else next((column for column in columns if "date" in column), "date")


def _resolve_columns(dataframe: DataFrame) -> dict[str, str]:
    """Resolve column names for PPD; include address fields for property_id when present."""
    columns = dataframe.columns
    base = {
        "price": _price_column(columns),
        "date": _date_column(columns),
        "postcode": next((column for column in columns if "postcode" in column), "postcode"),
        "property_type": next((column for column in columns if "property" in column and "type" in column), "property_type"),
        "duration": next((column for column in columns if "duration" in column), "duration"),
        "county": next((column for column in columns if "county" in column), "county"),
        "district": next((column for column in columns if "district" in column), "district"),
    }
    # Address fields for property-level grouping (PPD: PAON, SAON, Street)
    base["paon"] = next((column for column in columns if column == "paon" or "paon" in column), "paon")
    base["saon"] = next((column for column in columns if column == "saon" or "saon" in column), "saon")
    base["street"] = next((column for column in columns if "street" in column and "address" not in column.lower()), "street")
    return base


def _add_price_and_date(dataframe: DataFrame, column_map: dict[str, str]) -> DataFrame:
    dataframe = dataframe.withColumn("_price", F.col(column_map["price"]).cast("double")).filter(
        F.col("_price").isNotNull()
    )
    dataframe = dataframe.withColumn(
        "_date_of_transfer",
        F.to_date(F.substring(F.trim(F.col(column_map["date"])), 1, 10), "yyyy-MM-dd"),
    ).filter(F.col("_date_of_transfer").isNotNull())
    return dataframe.withColumn("_year_sold", F.year(F.col("_date_of_transfer")))


def _add_property_id(dataframe: DataFrame, column_map: dict[str, str]) -> DataFrame:
    """Add property_id = normalised(postcode|paon|saon|street) for grouping sales by property."""
    normalise_string_udf = F.udf(lambda val: normalise(val) if val else "", StringType())
    cols = dataframe.columns
    postcode = F.coalesce(normalise_string_udf(F.col(column_map["postcode"])), F.lit(""))
    paon = (
        F.coalesce(normalise_string_udf(F.col(column_map["paon"])), F.lit(""))
        if column_map["paon"] in cols
        else F.lit("")
    )
    saon = (
        F.coalesce(normalise_string_udf(F.col(column_map["saon"])), F.lit(""))
        if column_map["saon"] in cols
        else F.lit("")
    )
    street = (
        F.coalesce(normalise_string_udf(F.col(column_map["street"])), F.lit(""))
        if column_map["street"] in cols
        else F.lit("")
    )
    return dataframe.withColumn("property_id", F.concat_ws("|", postcode, paon, saon, street))


def _add_bought_year_and_price(dataframe: DataFrame) -> DataFrame:
    """Add _bought_date, _bought_price, _bought_year from previous sale of same property (lag 1)."""
    window = Window.partitionBy("property_id").orderBy(F.col("_date_of_transfer").asc(), F.col("_price").asc())
    dataframe = dataframe.withColumn("_bought_date", F.lag(F.col("_date_of_transfer"), 1).over(window))
    dataframe = dataframe.withColumn("_bought_price", F.lag(F.col("_price"), 1).over(window))
    return dataframe.withColumn("_bought_year", F.year(F.col("_bought_date")))


def _filter_repeat_sales(dataframe: DataFrame) -> DataFrame:
    """Keep only rows where this sale has a prior sale for the same property (for heatmap)."""
    filtered = dataframe.filter(F.col("_bought_year").isNotNull())
    return filtered.withColumn("_year_bought", F.col("_bought_year"))


def _add_appreciation_columns(dataframe: DataFrame) -> DataFrame:
    """Add _appreciation_pounds and _appreciation_pct for heatmap (sale - buy; % when buy > 0)."""
    df = dataframe.withColumn("_appreciation_pounds", F.col("_price") - F.col("_bought_price"))
    df = df.withColumn(
        "_appreciation_pct",
        F.when(F.col("_bought_price") > 0, (F.col("_price") - F.col("_bought_price")) / F.col("_bought_price") * 100),
    )
    return df


def _add_filter_and_location_columns(dataframe: DataFrame, column_map: dict[str, str]) -> DataFrame:
    """Add normalised filter and location columns to every row (no expansion yet)."""
    normalise_string_udf = F.udf(lambda val: normalise(val) if val else "unknown", StringType())
    normalise_property_type_udf = F.udf(normalise_property_type, StringType())
    normalise_duration_udf = F.udf(normalise_duration, StringType())
    normalise_postcode_prefix_udf = F.udf(normalise_postcode_prefix, StringType())
    dataframe = dataframe.withColumn(
        "_location_value", normalise_string_udf(F.col(column_map["county"]))
    )
    dataframe = dataframe.withColumn(
        "_post_prefix_value",
        normalise_postcode_prefix_udf(F.col(column_map["postcode"])),
    )
    dataframe = dataframe.withColumn(
        "_district_value",
        normalise_string_udf(F.col(column_map["district"])),
    )
    dataframe = dataframe.withColumn("_house_type", normalise_property_type_udf(F.col(column_map["property_type"])))
    dataframe = dataframe.withColumn("_tenure", normalise_duration_udf(F.col(column_map["duration"])))
    dataframe = dataframe.withColumn("_location_type", F.lit("county"))
    dataframe = dataframe.withColumn("_size_band", F.lit("all"))
    dataframe = dataframe.withColumn("_year_built_band", F.lit("all"))
    return dataframe


def _duplicate_rows_by_location_type(dataframe: DataFrame) -> DataFrame:
    """Duplicate each row for county, postcode and borough so each sale contributes to all location types."""
    df_county = dataframe  # already has _location_type=county, _location_value=county
    df_postcode = dataframe.withColumn("_location_type", F.lit("postcode")).withColumn(
        "_location_value", F.col("_post_prefix_value")
    )
    df_borough = dataframe.withColumn("_location_type", F.lit("borough")).withColumn(
        "_location_value", F.col("_district_value")
    )
    return df_county.unionByName(df_postcode).unionByName(df_borough)


def _expansion_table(spark: SparkSession) -> DataFrame:
    """DataFrame of 8 rows: (use_all_location, use_all_house_type, use_all_tenure) in {0,1}."""
    rows = [tuple(m) for m in EXPANSION_MASKS]
    return spark.createDataFrame(rows, ["_use_all_location", "_use_all_house_type", "_use_all_tenure"])


def _expand_to_all_permutations(dataframe: DataFrame) -> DataFrame:
    """Fan out each row into 8 rows: one per permutation where each filter can be value or 'all'."""
    spark = dataframe.sparkSession
    expansion = _expansion_table(spark)
    dataframe = dataframe.crossJoin(expansion)
    dataframe = dataframe.withColumn(
        "_expanded_location_value",
        F.when(F.col("_use_all_location") == 1, F.lit("all")).otherwise(F.col("_location_value")),
    )
    dataframe = dataframe.withColumn(
        "_expanded_house_type",
        F.when(F.col("_use_all_house_type") == 1, F.lit("all")).otherwise(F.col("_house_type")),
    )
    dataframe = dataframe.withColumn(
        "_expanded_tenure",
        F.when(F.col("_use_all_tenure") == 1, F.lit("all")).otherwise(F.col("_tenure")),
    )
    return dataframe.withColumn(
        "primary_key",
        F.concat_ws(
            "#",
            F.col("_location_type"),
            F.col("_expanded_location_value"),
            F.col("_expanded_house_type"),
            F.col("_expanded_tenure"),
            F.col("_size_band"),
            F.col("_year_built_band"),
        ),
    )


def _aggregate_by_primary_key_and_year_sold(dataframe: DataFrame) -> DataFrame:
    """Line path: group by (primary_key, _year_sold), compute avg price and count."""
    aggregated = dataframe.groupBy("primary_key", "_year_sold").agg(
        F.avg("_price").alias("avg_price"),
        F.count("*").alias("sale_count"),
    )
    return aggregated.withColumn("median_price", F.col("avg_price")).withColumn(
        "mode_price", F.col("avg_price")
    )


def _line_struct_sort_key():
    """Struct for collect_list so sort_array orders by year_sold (line path)."""
    return F.struct(
        F.col("_year_sold").cast(StringType()).alias("_year_sold"),
        F.col("avg_price"),
        F.col("median_price"),
        F.col("mode_price"),
        F.col("sale_count"),
    )


def _line_graph_udf(line_list: list) -> str:
    """Convert Spark Rows to LineGraphRow and build JSON. Used as UDF."""
    rows = [LineGraphRow.from_mapping(r) for r in (line_list or [])]
    return line_graph_json_from_list(rows)


def _build_line_result(by_primary_key_and_year: DataFrame) -> DataFrame:
    """Line path: per primary_key produce (primary_key, line_graph, sale_count)."""
    line_graph_user_defined_function = F.udf(_line_graph_udf, StringType())
    collected = by_primary_key_and_year.groupBy("primary_key").agg(
        F.sort_array(F.collect_list(_line_struct_sort_key())).alias("line_list"),
        F.sum("sale_count").alias("sale_count"),
    )
    return collected.withColumn("line_graph", line_graph_user_defined_function(F.col("line_list"))).select(
        "primary_key", "line_graph", "sale_count"
    )


def _aggregate_by_primary_key_year_bought_year_sold(dataframe: DataFrame) -> DataFrame:
    """Heatmap path: group by (primary_key, _year_bought, _year_sold); appreciation metrics + biggest winner/loser."""
    count_col = F.count("*")
    sum_appreciated = F.sum(F.when(F.col("_appreciation_pounds") > 0, 1).otherwise(0))
    aggregated = dataframe.groupBy("primary_key", "_year_bought", "_year_sold").agg(
        F.avg("_appreciation_pounds").alias("avg_appreciation_pounds"),
        F.expr("percentile_approx(_appreciation_pounds, 0.5, 10000)").alias("median_appreciation_pounds"),
        count_col.alias("sale_count"),
        F.avg("_appreciation_pct").alias("avg_appreciation_pct"),
        F.expr("percentile_approx(_appreciation_pct, 0.5, 10000)").alias("median_appreciation_pct"),
        (100 * sum_appreciated / count_col).alias("pct_sales_appreciated"),
        F.min("_appreciation_pounds").alias("biggest_loser_pounds"),
        F.min("_appreciation_pct").alias("biggest_loser_pct"),
        F.max("_appreciation_pounds").alias("biggest_winner_pounds"),
        F.max("_appreciation_pct").alias("biggest_winner_pct"),
    )
    return aggregated


def _heatmap_struct_sort_key():
    """Struct for collect_list so sort_array orders by year_bought, year_sold (heatmap path)."""
    return F.struct(
        F.col("_year_bought").cast(StringType()).alias("_year_bought"),
        F.col("_year_sold").cast(StringType()).alias("_year_sold"),
        F.col("avg_appreciation_pounds"),
        F.col("median_appreciation_pounds"),
        F.col("sale_count"),
        F.col("avg_appreciation_pct"),
        F.col("median_appreciation_pct"),
        F.col("pct_sales_appreciated"),
        F.col("biggest_loser_pounds"),
        F.col("biggest_loser_pct"),
        F.col("biggest_winner_pounds"),
        F.col("biggest_winner_pct"),
    )


def _heatmap_graph_udf(heatmap_list: list) -> str:
    """Convert Spark Rows to HeatmapRow and build JSON. Used as UDF."""
    rows = [HeatmapRow.from_mapping(r) for r in (heatmap_list or [])]
    return heatmap_graph_json_from_list(rows)


def _build_heatmap_result(by_primary_key_year_bought_year_sold: DataFrame) -> DataFrame:
    """Heatmap path: per primary_key produce (primary_key, heatmap_graph)."""
    heatmap_graph_user_defined_function = F.udf(_heatmap_graph_udf, StringType())
    collected = by_primary_key_year_bought_year_sold.groupBy("primary_key").agg(
        F.sort_array(F.collect_list(_heatmap_struct_sort_key())).alias("heatmap_list"),
    )
    return collected.withColumn("heatmap_graph", heatmap_graph_user_defined_function(F.col("heatmap_list"))).select(
        "primary_key", "heatmap_graph"
    )


def _build_performance_joined(line_result: DataFrame, heatmap_result: DataFrame) -> DataFrame:
    """Join line and heatmap on primary_key; build single value JSON per key."""
    combine_value_user_defined_function = F.udf(combine_line_and_heatmap_to_value, StringType())
    joined = line_result.join(heatmap_result, on="primary_key", how="left_outer")
    joined = joined.withColumn(
        "heatmap_graph",
        F.coalesce(F.col("heatmap_graph"), F.lit("[]")),
    )
    joined = joined.withColumn(
        "value",
        combine_value_user_defined_function(F.col("line_graph"), F.col("heatmap_graph"), F.col("sale_count")),
    )
    return joined.select(F.col("primary_key").alias("key"), F.col("value"))


def _build_dimension_index_in_spark(spark: SparkSession, performance: DataFrame) -> DataFrame:
    """From performance (key, value), derive distinct dimension index rows (meta#dim -> sk, label)."""
    keys = performance.select("key").distinct()
    key_parts = keys.withColumn("_key_parts", F.split(F.col("key"), "#"))
    # _key_parts: [location_type, location_value, house_type, tenure, size_band, year_built_band]
    dim_county = key_parts.filter(F.col("_key_parts").getItem(0) == "county").select(
        F.lit("meta#county").alias("key"),
        F.col("_key_parts").getItem(1).alias("sk"),
        F.initcap(F.col("_key_parts").getItem(1)).alias("label"),
    )
    dim_postcode = key_parts.filter(F.col("_key_parts").getItem(0) == "postcode").select(
        F.lit("meta#postcode").alias("key"),
        F.col("_key_parts").getItem(1).alias("sk"),
        F.upper(F.col("_key_parts").getItem(1)).alias("label"),
    )
    dim_borough = key_parts.filter(F.col("_key_parts").getItem(0) == "borough").select(
        F.lit("meta#borough").alias("key"),
        F.col("_key_parts").getItem(1).alias("sk"),
        F.initcap(F.col("_key_parts").getItem(1)).alias("label"),
    )
    dim_house_type = key_parts.select(
        F.lit("meta#house_type").alias("key"),
        F.col("_key_parts").getItem(2).alias("sk"),
        F.initcap(F.regexp_replace(F.col("_key_parts").getItem(2), "_", " ")).alias("label"),
    )
    dim_tenure = key_parts.select(
        F.lit("meta#tenure").alias("key"),
        F.col("_key_parts").getItem(3).alias("sk"),
        F.initcap(F.col("_key_parts").getItem(3)).alias("label"),
    )
    dim_size_band = key_parts.select(
        F.lit("meta#size_band").alias("key"),
        F.col("_key_parts").getItem(4).alias("sk"),
        F.col("_key_parts").getItem(4).alias("label"),
    )
    dim_year_built = key_parts.select(
        F.lit("meta#year_built_band").alias("key"),
        F.col("_key_parts").getItem(5).alias("sk"),
        F.col("_key_parts").getItem(5).alias("label"),
    )
    union = dim_county.unionByName(dim_postcode).unionByName(dim_borough).unionByName(dim_house_type).unionByName(dim_tenure).unionByName(
        dim_size_band
    ).unionByName(dim_year_built)
    # value = {"sk": sk, "label": label, "sale_count": null}
    union = union.withColumn(
        "value",
        F.to_json(F.struct(F.col("sk"), F.col("label"), F.lit(None).cast(StringType()).alias("sale_count"))),
    )
    return union.select("key", "value").dropDuplicates(["key", "value"])


def _resolve_output_base(config: dict[str, str]) -> Path:
    output_path = config["output_path"]
    return Path(output_path) if os.path.isabs(output_path) else REPO_ROOT / output_path


def _write_csv(dataframe: DataFrame, path: str) -> None:
    dataframe.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark ETL: PPD to house_price_performance and dimension_index.")
    parser.add_argument("--hackney", action="store_true", help="Filter initial dataset to borough Hackney only (district = Hackney).")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config = _load_config()
    input_path, _ = _resolve_paths(config)
    out_base = _resolve_output_base(config)
    spark = _create_spark()

    # 1. Read PPD and add price/date
    dataframe = _read_ppd(spark, input_path)
    column_map = _resolve_columns(dataframe)
    dataframe = _add_price_and_date(dataframe, column_map)

    if args.hackney:
        print("applying hackney filter")
        dataframe = dataframe.filter(F.lower(F.trim(F.col(column_map["district"]))) == "hackney")
        print(f"Applied hackney filter, {dataframe.count()} rows remaining")

    # 2. Add filter and location columns; add property_id (for heatmap path)
    dataframe = _add_filter_and_location_columns(dataframe, column_map)
    dataframe = _add_property_id(dataframe, column_map)

    # 3a. Line path: expand all sales -> aggregate by (primary_key, _year_sold) -> line_graph + sale_count
    line_expanded = _expand_to_all_permutations(_duplicate_rows_by_location_type(dataframe))
    by_primary_key_and_year_sold = _aggregate_by_primary_key_and_year_sold(line_expanded)
    line_result = _build_line_result(by_primary_key_and_year_sold)

    # 3b. Heatmap path: add bought_year/bought_price -> filter to repeat sales -> add appreciation cols -> expand -> aggregate by (primary_key, _year_bought, _year_sold)
    enriched = _add_bought_year_and_price(dataframe)
    repeat_sales = _filter_repeat_sales(enriched)
    repeat_sales = _add_appreciation_columns(repeat_sales)
    heatmap_expanded = _expand_to_all_permutations(_duplicate_rows_by_location_type(repeat_sales))
    by_primary_key_year_bought_year_sold = _aggregate_by_primary_key_year_bought_year_sold(
        heatmap_expanded
    )
    heatmap_result = _build_heatmap_result(by_primary_key_year_bought_year_sold)

    # 4. Join line and heatmap on primary_key; build value = { line_graph, heatmap_graph, sale_count }
    performance = _build_performance_joined(line_result, heatmap_result)

    # 5. Write house_price_performance CSV
    _write_csv(performance, str(out_base / "house_price_performance"))

    # 6. Build and write dimension_index from distinct keys
    dimension_index = _build_dimension_index_in_spark(spark, performance)
    _write_csv(dimension_index, str(out_base / "dimension_index"))

    spark.stop()
    print("ETL done. Output:", config["output_path"])


if __name__ == "__main__":
    main()
