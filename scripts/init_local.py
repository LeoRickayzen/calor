#!/usr/bin/env python3
"""
Create DynamoDB tables and seed them for local dev (DynamoDB Local).
Uses HOUSES_DYNAMODB_ENDPOINT_URL (default http://localhost:8001). Run from repo root:
  HOUSES_DYNAMODB_ENDPOINT_URL=http://localhost:8001 python scripts/init_local.py
Or: cd backend && uv run python ../scripts/init_local.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Default endpoint so serve.sh and local dev work without extra env
os.environ.setdefault("HOUSES_DYNAMODB_ENDPOINT_URL", "http://localhost:8001")

# Run from repo root: backend must be on path
ROOT = Path(__file__).resolve().parent.parent
BACKEND = ROOT / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
os.chdir(BACKEND)

from app.config import settings
from app.db.client import get_dynamo_client, get_dynamo_resource
from app.db.compression import compress_graph_data
from app.db.tables import ensure_tables


def seed_house_performance(table) -> None:
    """Insert sample performance items. One row at county=all (default filters), plus per-location rows."""
    # Aggregate row: location_value and dimensions all "all" — returned when user keeps default filters
    line_all = [
        {"year_sold": "2020", "avg_price": 480000, "median_price": 470000, "mode_price": 450000, "sale_count": 142},
        {"year_sold": "2021", "avg_price": 510000, "median_price": 500000, "mode_price": 490000, "sale_count": 158},
    ]
    heat_all = [
        {"year_bought": "2020", "year_sold": "2020", "avg_price": 480000, "median_price": 470000, "sale_count": 142},
        {"year_bought": "2021", "year_sold": "2021", "avg_price": 510000, "median_price": 500000, "sale_count": 158},
    ]
    table.put_item(
        Item={
            "pk": "county#all#all#all#all#all",
            "line_graph": compress_graph_data(line_all),
            "heatmap_graph": compress_graph_data(heat_all),
            "sale_count": 300,
        }
    )
    table.put_item(
        Item={
            "pk": "county#Greater London#flat#freehold#50_75#1990_1999",
            "line_graph": compress_graph_data(line_all),
            "heatmap_graph": compress_graph_data(heat_all),
            "sale_count": 300,
        }
    )
    line_terraced = [
        {"year_sold": "2020", "avg_price": 520000, "median_price": 510000, "mode_price": 500000, "sale_count": 89},
    ]
    heat_terraced = [
        {"year_bought": "2020", "year_sold": "2020", "avg_price": 520000, "median_price": 510000, "sale_count": 89},
    ]
    table.put_item(
        Item={
            "pk": "county#Greater London#terraced#leasehold#75_100#1980_1990",
            "line_graph": compress_graph_data(line_terraced),
            "heatmap_graph": compress_graph_data(heat_terraced),
            "sale_count": 89,
        }
    )


def seed_dimension_index(table) -> None:
    """Insert dimension values for filter dropdowns (county, house_type, tenure, size_band, year_built_band). 'all' must be in DB for the no-filter option."""
    for value, label in [("all", "All"), ("Greater London", "Greater London"), ("West Yorkshire", "West Yorkshire")]:
        table.put_item(Item={"pk": "meta#county", "sk": value, "label": label, "sale_count": 1000 if value != "all" else None})
    for value in ["all", "flat", "terraced", "detached"]:
        table.put_item(Item={"pk": "meta#house_type", "sk": value, "label": "All" if value == "all" else value})
    for value in ["all", "freehold", "leasehold"]:
        table.put_item(Item={"pk": "meta#tenure", "sk": value, "label": "All" if value == "all" else value})
    for value in ["all", "50_75", "75_100", "100_125"]:
        table.put_item(Item={"pk": "meta#size_band", "sk": value, "label": "All" if value == "all" else value})
    for value in ["all", "1980_1990", "1990_1999", "2000_2010"]:
        table.put_item(Item={"pk": "meta#year_built_band", "sk": value, "label": "All" if value == "all" else value})


def main() -> None:
    endpoint = os.environ.get("HOUSES_DYNAMODB_ENDPOINT_URL", "")
    print(f"DynamoDB endpoint: {endpoint or '(default AWS)'}")
    print("Creating tables...")
    ensure_tables()
    print("Seeding house_price_performance...")
    resource = get_dynamo_resource()
    seed_house_performance(resource.Table(settings.table_house_price_performance))
    print("Seeding dimension_index...")
    seed_dimension_index(resource.Table(settings.table_dimension_index))
    print("Done.")


if __name__ == "__main__":
    main()
