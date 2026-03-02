"""Integration tests for dimensions API against DynamoDB Docker."""

import pytest
from httpx import AsyncClient


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_counties(api_client: AsyncClient) -> None:
    """GET /api/dimensions/county returns county filter values."""
    r = await api_client.get("/api/dimensions/county")
    assert r.status_code == 200
    data = r.json()
    assert data["dimension_name"] == "county"
    assert "values" in data
    assert len(data["values"]) == 3  # "all" + 2 counties from seed
    values = {v["value"] for v in data["values"]}
    assert "all" in values
    assert "Greater London" in values
    assert "West Yorkshire" in values
    assert data["values"][0]["value"] == "all"
    assert data["values"][0]["label"] == "All"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_house_types(api_client: AsyncClient) -> None:
    """GET /api/dimensions/house_type returns house type filter values."""
    r = await api_client.get("/api/dimensions/house_type")
    assert r.status_code == 200
    data = r.json()
    assert data["dimension_name"] == "house_type"
    assert len(data["values"]) == 4  # "all" + 3 house types from seed
    values = {v["value"] for v in data["values"]}
    assert "all" in values
    assert "flat" in values
    assert "terraced" in values
    assert "detached" in values
