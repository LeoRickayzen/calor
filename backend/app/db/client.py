"""DynamoDB client and resource factories."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from app.config import settings


def get_dynamo_client() -> BaseClient:
    """Return a DynamoDB client (use endpoint_url for local)."""
    kwargs: dict[str, Any] = {
        "region_name": settings.dynamodb_region,
        "aws_access_key_id": "dummy",
        "aws_secret_access_key": "dummy",
    }
    if settings.dynamodb_endpoint_url and settings.dynamodb_endpoint_url.strip():
        kwargs["endpoint_url"] = settings.dynamodb_endpoint_url.strip()
    return boto3.client("dynamodb", **kwargs)


def get_dynamo_resource() -> Any:
    """Return a DynamoDB resource (use endpoint_url for local)."""
    kwargs: dict[str, Any] = {
        "region_name": settings.dynamodb_region,
        "aws_access_key_id": "dummy",
        "aws_secret_access_key": "dummy",
    }
    if settings.dynamodb_endpoint_url and settings.dynamodb_endpoint_url.strip():
        kwargs["endpoint_url"] = settings.dynamodb_endpoint_url.strip()
    return boto3.resource("dynamodb", **kwargs)


def dynamo_client_fixture() -> Generator[BaseClient, None, None]:
    """Fixture-friendly: yield client (for tests that inject endpoint)."""
    yield get_dynamo_client()
