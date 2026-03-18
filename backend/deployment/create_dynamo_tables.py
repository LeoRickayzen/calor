"""
Create DynamoDB tables for the houses backend in real AWS.

Idempotent: run once per account/region; tables that already exist are left unchanged.
Uses the default AWS credential chain (env vars, ~/.aws/credentials, or AWS_PROFILE).

Usage (from backend dir):
  python deployment/create_dynamo_tables.py
  uv run python deployment/create_dynamo_tables.py

Override region: AWS_REGION=us-east-1 python deployment/create_dynamo_tables.py
Or: HOUSES_DYNAMODB_REGION=us-east-1 python deployment/create_dynamo_tables.py
"""

from __future__ import annotations

import argparse
import os
import sys

import boto3
from botocore.exceptions import ClientError


def _resource_not_found(e: Exception) -> bool:
    if isinstance(e, ClientError):
        return e.response.get("Error", {}).get("Code") == "ResourceNotFoundException"
    return False


def get_table_configs(
    table_performance: str,
    table_dimension_index: str,
) -> list[dict]:
    return [
        {
            "TableName": table_performance,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
        },
        {
            "TableName": table_dimension_index,
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        },
    ]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create DynamoDB tables (house_price_performance, dimension_index) in AWS.",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION") or os.environ.get("HOUSES_DYNAMODB_REGION") or "eu-west-2",
        help="AWS region (default: AWS_REGION or HOUSES_DYNAMODB_REGION or eu-west-2)",
    )
    args = parser.parse_args()
    region = args.region

    table_performance = os.environ.get(
        "HOUSES_TABLE_HOUSE_PRICE_PERFORMANCE",
        "house_price_performance",
    )
    table_dimension_index = os.environ.get(
        "HOUSES_TABLE_DIMENSION_INDEX",
        "dimension_index",
    )

    client = boto3.client("dynamodb", region_name=region)
    configs = get_table_configs(table_performance, table_dimension_index)

    created: list[str] = []
    existed: list[str] = []

    for cfg in configs:
        name = cfg["TableName"]
        try:
            client.describe_table(TableName=name)
            existed.append(name)
        except Exception as e:
            if _resource_not_found(e):
                client.create_table(**cfg)
                created.append(name)
            else:
                print(f"Error checking table {name}: {e}", file=sys.stderr)
                raise

    print(f"Region: {region}")
    if created:
        print(f"Created: {', '.join(created)}")
    if existed:
        print(f"Already existed: {', '.join(existed)}")


if __name__ == "__main__":
    main()
