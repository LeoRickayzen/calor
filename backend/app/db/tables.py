"""DynamoDB table definitions and creation (for local/test)."""

from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError

from app.config import settings
from app.db.client import get_dynamo_client


def _is_resource_not_found(e: Exception) -> bool:
    """True if exception is DynamoDB ResourceNotFoundException."""
    if isinstance(e, ClientError):
        return e.response.get("Error", {}).get("Code") == "ResourceNotFoundException"
    return False


def ensure_tables(client: Any | None = None) -> None:
    """
    Create tables if they do not exist. Idempotent.
    Use the provided client or the default (from config).
    """
    c = client or get_dynamo_client()

    tables_config: list[dict[str, Any]] = [
        {
            "TableName": settings.table_house_price_performance,
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        },
        {
            "TableName": settings.table_dimension_index,
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

    for cfg in tables_config:
        name = cfg["TableName"]
        try:
            c.describe_table(TableName=name)
        except Exception as e:
            if _is_resource_not_found(e):
                c.create_table(**cfg)
            else:
                raise
