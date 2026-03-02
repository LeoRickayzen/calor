"""Integration tests for performance API against DynamoDB Docker."""

import pytest
from httpx import AsyncClient


@pytest.mark.integration
@pytest.mark.asyncio
async def test_health(api_client: AsyncClient) -> None:
    """Health endpoint returns ok."""
    r = await api_client.get("/health")
    assert r.status_code == 200
    data: dict = r.json()
    assert data == {"status": "ok"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_performance_default_filters(api_client: AsyncClient) -> None:
    """GET /api/performance with default filters (all) returns empty when no aggregate row exists."""
    r = await api_client.get(
        "/api/performance",
        params={"location_type": "county", "location_value": "Greater London"},
    )
    assert r.status_code == 200
    data: dict = r.json()
    assert data == {"items": [], "count": 0}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_performance_with_filter_all(api_client: AsyncClient) -> None:
    """GET /api/performance with house_type=all returns same as omitting param (default all)."""
    r_all = await api_client.get(
        "/api/performance",
        params={
            "location_type": "county",
            "location_value": "Greater London",
            "house_type": "all",
        },
    )
    r_no_param = await api_client.get(
        "/api/performance",
        params={"location_type": "county", "location_value": "Greater London"},
    )
    assert r_all.status_code == 200
    assert r_no_param.status_code == 200
    expected: dict = {"items": [], "count": 0}
    assert r_all.json() == expected
    assert r_no_param.json() == expected


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_performance_exact_dimensions(api_client: AsyncClient) -> None:
    """GET /api/performance with exact dimensions returns the seeded item (flat, freehold, 50_75, 1990_1999)."""
    r = await api_client.get(
        "/api/performance",
        params={
            "location_type": "county",
            "location_value": "Greater London",
            "house_type": "flat",
            "tenure": "freehold",
            "size_band": "50_75",
            "year_built_band": "1990_1999",
        },
    )
    assert r.status_code == 200
    data: dict = r.json()
    expected = {
        "items": [
            {
                "line_graph": [
                    {
                        "year_sold": "2020",
                        "avg_price": 480000,
                        "median_price": 470000,
                        "mode_price": 450000,
                        "sale_count": 142,
                    },
                    {
                        "year_sold": "2021",
                        "avg_price": 510000,
                        "median_price": 500000,
                        "mode_price": 490000,
                        "sale_count": 158,
                    },
                ],
                "heatmap_graph": [
                    {
                        "year_bought": "2020",
                        "year_sold": "2020",
                        "avg_appreciation_pounds": 0,
                        "median_appreciation_pounds": 0,
                        "sale_count": 142,
                        "avg_appreciation_pct": None,
                        "median_appreciation_pct": None,
                        "pct_sales_appreciated": 0,
                    },
                    {
                        "year_bought": "2021",
                        "year_sold": "2021",
                        "avg_appreciation_pounds": 30000,
                        "median_appreciation_pounds": 28000,
                        "sale_count": 158,
                        "avg_appreciation_pct": 6.25,
                        "median_appreciation_pct": 5.9,
                        "pct_sales_appreciated": 95.0,
                    },
                ],
                "sale_count": 300,
            }
        ],
        "count": 1,
    }
    assert data == expected


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_performance_empty_location(api_client: AsyncClient) -> None:
    """GET /api/performance for unknown location returns empty list."""
    r = await api_client.get(
        "/api/performance",
        params={"location_type": "county", "location_value": "Unknown County"},
    )
    assert r.status_code == 200
    data: dict = r.json()
    assert data == {"items": [], "count": 0}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_line_graph_no_match(api_client: AsyncClient) -> None:
    """GET /api/performance/line with default filters returns empty series when no aggregate row."""
    r = await api_client.get(
        "/api/performance/line",
        params={"location_type": "county", "location_value": "Greater London"},
    )
    assert r.status_code == 200
    data: dict = r.json()
    assert data == {"series": []}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_line_graph_exact_dimensions(api_client: AsyncClient) -> None:
    """GET /api/performance/line with exact dimensions returns that item's line series."""
    r = await api_client.get(
        "/api/performance/line",
        params={
            "location_type": "county",
            "location_value": "Greater London",
            "house_type": "flat",
            "tenure": "freehold",
            "size_band": "50_75",
            "year_built_band": "1990_1999",
        },
    )
    assert r.status_code == 200
    data: dict = r.json()
    expected = {
        "series": [
            {
                "year_sold": "2020",
                "avg_price": 480000,
                "median_price": 470000,
                "mode_price": 450000,
                "sale_count": 142,
            },
            {
                "year_sold": "2021",
                "avg_price": 510000,
                "median_price": 500000,
                "mode_price": 490000,
                "sale_count": 158,
            },
        ]
    }
    assert data == expected


@pytest.mark.integration
@pytest.mark.asyncio
async def test_heatmap_no_match(api_client: AsyncClient) -> None:
    """GET /api/performance/heatmap with default filters returns empty cells when no aggregate row."""
    r = await api_client.get(
        "/api/performance/heatmap",
        params={
            "location_type": "county",
            "location_value": "Greater London",
        },
    )
    assert r.status_code == 200
    data: dict = r.json()
    assert data == {"cells": []}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_heatmap_exact_dimensions(api_client: AsyncClient) -> None:
    """GET /api/performance/heatmap with exact dimensions returns that item's heatmap cells."""
    r = await api_client.get(
        "/api/performance/heatmap",
        params={
            "location_type": "county",
            "location_value": "Greater London",
            "house_type": "flat",
            "tenure": "freehold",
            "size_band": "50_75",
            "year_built_band": "1990_1999",
        },
    )
    assert r.status_code == 200
    data: dict = r.json()
    expected = {
        "cells": [
            {
                "year_bought": "2020",
                "year_sold": "2020",
                "avg_appreciation_pounds": 0,
                "median_appreciation_pounds": 0,
                "sale_count": 142,
                "avg_appreciation_pct": None,
                "median_appreciation_pct": None,
                "pct_sales_appreciated": 0,
            },
            {
                "year_bought": "2021",
                "year_sold": "2021",
                "avg_appreciation_pounds": 30000,
                "median_appreciation_pounds": 28000,
                "sale_count": 158,
                "avg_appreciation_pct": 6.25,
                "median_appreciation_pct": 5.9,
                "pct_sales_appreciated": 95.0,
            },
        ]
    }
    assert data == expected
