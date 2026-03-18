#!/usr/bin/env python3
"""
Load (key, value) CSVs produced by the Spark ETL into DynamoDB.
Schema matches ETL output: key + value (JSON with line_graph/heatmap_graph for performance;
sk/label for dimension_index). Uses fixed defaults: CSV path etl/output/; DynamoDB Local at 8001.
Use --remote to write to real AWS tables (same region/table names as backend; uses default AWS credentials).
Run from repo root: python etl/scripts/load_csv_to_dynamodb.py [--remote]
"""
from __future__ import annotations

import argparse
import base64
import json
import logging
import sys
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def _compress_graph_data(data: list) -> str:
    """Compress line_graph/heatmap_graph list to base64+zlib string for DynamoDB."""
    if not data:
        return base64.b64encode(zlib.compress(b"[]")).decode("ascii")

    def _json_default(o):
        if isinstance(o, Decimal):
            return float(o)
        raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

    return base64.b64encode(
        zlib.compress(json.dumps(data, default=_json_default).encode("utf-8"))
    ).decode("ascii")


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


DYNAMODB_REGION = "us-east-1"


def _get_dynamo_resource(remote: bool):
    """DynamoDB resource: local at 8001 if not remote; real AWS (default credentials) if remote."""
    if remote:
        return boto3.resource("dynamodb", region_name=DYNAMODB_REGION)
    return boto3.resource(
        "dynamodb",
        endpoint_url=DYNAMODB_ENDPOINT_URL,
        region_name=DYNAMODB_REGION,
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
    )


def load_performance_csv(resource, csv_dir: Path, workers: int | None = None) -> int:
    """Load house_price_performance CSVs. ETL writes columns: key, value (JSON with line_graph, heatmap_graph). If workers is set, use that many threads for put_item."""
    table = resource.Table(TABLE_HOUSE_PRICE_PERFORMANCE)
    count = 0
    logger = logging.getLogger(__name__)
    for path in sorted(csv_dir.glob("*.csv")):
        dataframe = _read_key_value_csv(path)
        logger.info("Read %d rows from %s", len(dataframe), path.name)
        if dataframe.empty:
            continue
        items = []
        n_rows = len(dataframe)
        for _, row in tqdm(dataframe.iterrows(), total=n_rows, desc=f"read {path.name}", unit="row"):
            key = (row.get("key") or "").strip()
            val_str = (row.get("value") or "").strip()
            if not key:
                continue
            try:
                parsed = json.loads(val_str)
            except json.JSONDecodeError:
                continue
            line_data = _decimalize(parsed.get("line_graph", []))
            heatmap_data = _decimalize(parsed.get("heatmap_graph", []))
            items.append({
                "pk": key,
                "line_graph": _compress_graph_data(line_data),
                "heatmap_graph": _compress_graph_data(heatmap_data),
            })
        if not items:
            continue
        n = len(items)
        logger.info("Writing %d items from %s to %s%s", n, path.name, TABLE_HOUSE_PRICE_PERFORMANCE, f" (workers={workers})" if workers else "")
        if workers:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(table.put_item, Item=item) for item in items]
                for f in tqdm(as_completed(futures), total=n, desc=path.name, unit="row"):
                    f.result()
        else:
            for item in tqdm(items, desc=path.name, unit="row"):
                table.put_item(Item=item)
        count += n
    logger.info("Finished %s: %d items total", TABLE_HOUSE_PRICE_PERFORMANCE, count)
    return count


def load_dimension_index_csv(resource, csv_dir: Path, workers: int | None = None) -> int:
    """Load dimension_index CSVs. ETL writes columns: key, value (JSON with sk, label). If workers is set, use that many threads for put_item."""
    table = resource.Table(TABLE_DIMENSION_INDEX)
    count = 0
    logger = logging.getLogger(__name__)
    for path in sorted(csv_dir.glob("*.csv")):
        dataframe = _read_key_value_csv(path)
        logger.info("Read %d rows from %s", len(dataframe), path.name)
        if dataframe.empty:
            continue
        items = []
        for _, row in dataframe.iterrows():
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
            items.append(item)
        if not items:
            continue
        n = len(items)
        logger.info("Writing %d items from %s to %s%s", n, path.name, TABLE_DIMENSION_INDEX, f" (workers={workers})" if workers else "")
        if workers:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(table.put_item, Item=item) for item in items]
                for f in tqdm(as_completed(futures), total=n, desc=path.name, unit="row"):
                    f.result()
        else:
            for item in tqdm(items, desc=path.name, unit="row"):
                table.put_item(Item=item)
        count += n
    logger.info("Finished %s: %d items total", TABLE_DIMENSION_INDEX, count)
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Load ETL output CSVs into DynamoDB.")
    parser.add_argument(
        "--remote",
        action="store_true",
        help="Write to real AWS DynamoDB tables (default: write to Local at 8001)",
    )
    parser.add_argument(
        "--multi-thread",
        action="store_true",
        help="Use 10 threads for put_item (faster for large loads)",
    )
    args = parser.parse_args()
    workers = 10 if args.multi_thread else None
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

    resource = _get_dynamo_resource(remote=args.remote)
    if args.remote:
        logging.getLogger(__name__).info("Writing to remote DynamoDB (AWS)")
    dimension_count = load_dimension_index_csv(resource, dimension_dir, workers=workers)
    performance_count = load_performance_csv(resource, performance_dir, workers=workers)
    print("Loaded", performance_count, "house_price_performance items and", dimension_count, "dimension_index items.")


if __name__ == "__main__":
    main()
