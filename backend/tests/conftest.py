"""Pytest fixtures: DynamoDB (manual via docker-compose), tables, seeded data, FastAPI client."""

import os
from collections.abc import Generator
import pytest
from httpx import ASGITransport, AsyncClient

from app.api.routes.dimensions import get_repo as get_dimensions_repo
from app.api.routes.performance import get_repo as get_performance_repo
from app.config import settings
from app.db.repository import PerformanceRepository
from app.db.tables import ensure_tables
from app.main import app


def _dynamodb_endpoint() -> str:
    """DynamoDB endpoint from env. Defaults to localhost:8000 (docker-compose)."""
    return os.environ.get("HOUSES_DYNAMODB_ENDPOINT_URL", "http://localhost:8000").strip()


@pytest.fixture(scope="session")
def dynamodb_endpoint_url() -> Generator[str, None, None]:
    """
    Endpoint URL for DynamoDB. Set HOUSES_DYNAMODB_ENDPOINT_URL or use default
    http://localhost:8000 (start DynamoDB with: docker-compose up -d).
    """
    yield _dynamodb_endpoint()


@pytest.fixture(scope="session")
def dynamo_client(dynamodb_endpoint_url: str):
    """Boto3 DynamoDB client pointing at DynamoDB Local."""
    import boto3

    return boto3.client(
        "dynamodb",
        endpoint_url=dynamodb_endpoint_url,
        region_name="eu-west-2",
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
    )


@pytest.fixture(scope="session")
def dynamo_resource(dynamodb_endpoint_url: str):
    """Boto3 DynamoDB resource pointing at DynamoDB Local."""
    import boto3

    return boto3.resource(
        "dynamodb",
        endpoint_url=dynamodb_endpoint_url,
        region_name="eu-west-2",
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
    )


@pytest.fixture(scope="session")
def tables_created(dynamo_client) -> None:
    """Create tables in DynamoDB. Run once per session."""
    ensure_tables(dynamo_client)


@pytest.fixture
def seed_house_performance(dynamo_resource, tables_created) -> None:
    """Insert sample rows into house_price_performance. One item per (location, dimensions); pk only; line_graph + heatmap_graph."""
    table = dynamo_resource.Table(settings.table_house_price_performance)

    # Item 1: flat, freehold, 50_75, 1990_1999 in Greater London
    table.put_item(
        Item={
            "pk": "county#Greater London#flat#freehold#50_75#1990_1999",
            "line_graph": [
                {"year_sold": "2020", "avg_price": 480000, "median_price": 470000, "mode_price": 450000, "sale_count": 142},
                {"year_sold": "2021", "avg_price": 510000, "median_price": 500000, "mode_price": 490000, "sale_count": 158},
            ],
            "heatmap_graph": [
                {"year_bought": "2020", "year_sold": "2020", "avg_appreciation_pounds": 0, "median_appreciation_pounds": 0, "sale_count": 142, "pct_sales_appreciated": 0},
                {"year_bought": "2021", "year_sold": "2021", "avg_appreciation_pounds": 30000, "median_appreciation_pounds": 28000, "sale_count": 158, "avg_appreciation_pct": 6.25, "median_appreciation_pct": 5.9, "pct_sales_appreciated": 95.0},
            ],
            "sale_count": 300,
        }
    )

    # Item 2: terraced, leasehold, 75_100, 1980_1990 in Greater London
    table.put_item(
        Item={
            "pk": "county#Greater London#terraced#leasehold#75_100#1980_1990",
            "line_graph": [
                {"year_sold": "2020", "avg_price": 520000, "median_price": 510000, "mode_price": 500000, "sale_count": 89},
            ],
            "heatmap_graph": [
                {"year_bought": "2020", "year_sold": "2020", "avg_appreciation_pounds": 0, "median_appreciation_pounds": 0, "sale_count": 89, "pct_sales_appreciated": 0},
            ],
            "sale_count": 89,
        }
    )


@pytest.fixture
def seed_dimension_index(dynamo_resource, tables_created) -> None:
    """Insert sample dimension index rows for filter dropdowns."""
    table = dynamo_resource.Table(settings.table_dimension_index)
    for value, label in [("Greater London", "Greater London"), ("West Yorkshire", "West Yorkshire")]:
        table.put_item(Item={"pk": "meta#county", "sk": value, "label": label, "sale_count": 1000})
    for value in ["flat", "terraced", "detached"]:
        table.put_item(Item={"pk": "meta#house_type", "sk": value, "label": value})


@pytest.fixture
async def api_client(
    dynamo_resource,
    tables_created: None,
    seed_house_performance: None,
    seed_dimension_index: None,
) -> AsyncClient:
    """
    HTTP client for the FastAPI app. Override dependencies to use DynamoDB
    resource; tables are created and seeded.
    """
    repo = PerformanceRepository(resource=dynamo_resource)
    app.dependency_overrides[get_performance_repo] = lambda: repo
    app.dependency_overrides[get_dimensions_repo] = lambda: repo
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client
    finally:
        app.dependency_overrides.clear()
