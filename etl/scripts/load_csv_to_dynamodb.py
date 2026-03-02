#!/usr/bin/env python3
"""
Load (key, value) CSVs produced by the Spark ETL into DynamoDB.
Schema matches ETL output: key + value (JSON with line_graph/heatmap_graph for performance;
sk/label for dimension_index). Uses fixed defaults: CSV path etl/output/; DynamoDB Local at 8001.
Run from repo root: python etl/scripts/load_csv_to_dynamodb.py
"""
from __future__ import annotations

import json
import logging
import sys
from decimal import Decimal
from pathlib import Path

import boto3
import pandas as pd
from tqdm import tqdm

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CSV_OUTPUT = REPO_ROOT / "etl" / "output"
DYNAMODB_ENDPOINT_URL = "http://localhost:8001"
TABLE_HOUSE_PRICE_PERFORMANCE = "house_price_performance"
TABLE_DIMENSION_INDEX = "dimension_index"


def _decimalize(obj):
    """Recursively convert Python int/float to Decimal for DynamoDB."""
    if isinstance(obj, dict):
        return {k: _decimalize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decimalize(x) for x in obj]
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, int) and not isinstance(obj, bool):
        return Decimal(obj)
    return obj


def _read_key_value_csv(path: Path) -> pd.DataFrame:
    """Read ETL key,value CSV: split each line on first comma only so JSON value (with embedded commas) stays intact."""
    records = []
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    if len(lines) < 2:
        return pd.DataFrame()
    for line in lines[1:]:  # skip header
        line = line.rstrip("\n")
        first_comma = line.find(",")
        if first_comma < 0:
            continue
        key = line[:first_comma].strip().strip('"')
        value = line[first_comma + 1 :].strip()
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1].replace('\\"', '"')
        records.append({"key": key, "value": value})
    return pd.DataFrame(records)


def _get_dynamo_resource():
    """DynamoDB resource for Local at hardcoded endpoint."""
    return boto3.resource(
        "dynamodb",
        endpoint_url=DYNAMODB_ENDPOINT_URL,
        region_name="eu-west-2",
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
    )


def load_performance_csv(resource, csv_dir: Path) -> int:
    """Load house_price_performance CSVs. ETL writes columns: key, value (JSON with line_graph, heatmap_graph)."""
    table = resource.Table(TABLE_HOUSE_PRICE_PERFORMANCE)
    count = 0
    logger = logging.getLogger(__name__)
    for path in sorted(csv_dir.glob("*.csv")):
        dataframe = _read_key_value_csv(path)
        if dataframe.empty:
            continue
        n = len(dataframe)
        logger.info("Writing %d items from %s to %s", n, path.name, TABLE_HOUSE_PRICE_PERFORMANCE)
        for _, row in tqdm(dataframe.iterrows(), total=n, desc=path.name, unit="row"):
            key = (row.get("key") or "").strip()
            val_str = (row.get("value") or "").strip()
            if not key:
                continue
            try:
                parsed = json.loads(val_str)
            except json.JSONDecodeError:
                continue
            item = {
                "pk": key,
                "line_graph": _decimalize(parsed.get("line_graph", [])),
                "heatmap_graph": _decimalize(parsed.get("heatmap_graph", [])),
            }
            table.put_item(Item=item)
            logger.debug("Wrote pk=%s", key)
            count += 1
    logger.info("Finished %s: %d items total", TABLE_HOUSE_PRICE_PERFORMANCE, count)
    return count


def load_dimension_index_csv(resource, csv_dir: Path) -> int:
    """Load dimension_index CSVs. ETL writes columns: key, value (JSON with sk, label)."""
    table = resource.Table(TABLE_DIMENSION_INDEX)
    count = 0
    logger = logging.getLogger(__name__)
    for path in sorted(csv_dir.glob("*.csv")):
        dataframe = _read_key_value_csv(path)
        if dataframe.empty:
            continue
        n = len(dataframe)
        logger.info("Writing %d items from %s to %s", n, path.name, TABLE_DIMENSION_INDEX)
        for _, row in tqdm(dataframe.iterrows(), total=n, desc=path.name, unit="row"):
            primary_key = (row.get("key") or "").strip()
            val_str = (row.get("value") or "").strip()
            if not primary_key:
                continue
            try:
                parsed = json.loads(val_str)
            except json.JSONDecodeError:
                continue
            sort_key = parsed.get("sk") or ""
            if not sort_key:
                continue
            item = {
                "pk": primary_key,
                "sk": sort_key,
                "label": parsed.get("label"),
            }
            item = {k: v for k, v in item.items() if v is not None}
            table.put_item(Item=item)
            logger.debug("Wrote pk=%s sk=%s label=%s", primary_key, sort_key, item.get("label"))
            count += 1
    logger.info("Finished %s: %d items total", TABLE_DIMENSION_INDEX, count)
    return count


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    base = DEFAULT_CSV_OUTPUT
    performance_dir = base / "house_price_performance"
    dimension_dir = base / "dimension_index"

    if not performance_dir.exists():
        print("Not found:", performance_dir)
        sys.exit(1)
    if not dimension_dir.exists():
        print("Not found:", dimension_dir)
        sys.exit(1)

    resource = _get_dynamo_resource()
    performance_count = load_performance_csv(resource, performance_dir)
    dimension_count = load_dimension_index_csv(resource, dimension_dir)
    print("Loaded", performance_count, "house_price_performance items and", dimension_count, "dimension_index items.")


if __name__ == "__main__":
    main()
