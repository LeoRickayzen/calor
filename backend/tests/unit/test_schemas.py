"""Unit tests for Pydantic schemas (no DynamoDB required)."""

import pytest

from app.models.schemas import (
    HousePricePerformanceItem,
    LineGraphPoint,
    HeatmapCell,
    DimensionIndexItem,
    LocationType,
    LineGraphPointStored,
    HeatmapCellStored,
)


def test_house_price_performance_from_dynamo() -> None:
    """HousePricePerformanceItem.from_dynamo parses value attributes only (line_graph, heatmap_graph, sale_count)."""
    item = {
        "pk": "county#Greater London#flat#freehold#50_75#1990_1999",
        "sk": "agg",
        "line_graph": [
            {"year_sold": "2020", "avg_price": 480000, "median_price": 470000, "mode_price": 450000, "sale_count": 142},
        ],
        "heatmap_graph": [
            {"year_bought": "2020", "year_sold": "2020", "avg_appreciation_pounds": 10000, "median_appreciation_pounds": 9000, "sale_count": 142, "avg_appreciation_pct": 2.5, "median_appreciation_pct": 2.0, "pct_sales_appreciated": 90},
        ],
        "sale_count": 142,
    }
    parsed = HousePricePerformanceItem.from_dynamo(item)
    assert len(parsed.line_graph) == 1
    assert parsed.line_graph[0].year_sold == "2020"
    assert parsed.line_graph[0].median_price == 470000
    assert parsed.line_graph[0].sale_count == 142
    assert len(parsed.heatmap_graph) == 1
    assert parsed.heatmap_graph[0].year_bought == "2020"
    assert parsed.heatmap_graph[0].year_sold == "2020"
    assert parsed.heatmap_graph[0].avg_appreciation_pounds == 10000
    assert parsed.heatmap_graph[0].sale_count == 142
    assert parsed.sale_count == 142


def test_line_graph_point() -> None:
    """LineGraphPoint uses year_sold and optional metrics."""
    p = LineGraphPoint(year_sold="2020", median_price=100.0, sale_count=5)
    assert p.year_sold == "2020"
    assert p.median_price == 100.0
    assert p.mode_price is None
    assert p.sale_count == 5


def test_heatmap_cell() -> None:
    """HeatmapCell has year_bought, year_sold, and appreciation metrics."""
    c = HeatmapCell(
        year_bought="2020",
        year_sold="2021",
        avg_appreciation_pounds=30000,
        median_appreciation_pounds=29000,
        sale_count=10,
        avg_appreciation_pct=5.0,
        median_appreciation_pct=4.5,
        pct_sales_appreciated=92.0,
    )
    assert c.year_bought == "2020"
    assert c.year_sold == "2021"
    assert c.median_appreciation_pounds == 29000
    assert c.sale_count == 10


def test_dimension_index_item() -> None:
    """DimensionIndexItem allows optional label and sale_count."""
    d = DimensionIndexItem(dimension_name="county", value="Greater London", label="Greater London", sale_count=1000)
    assert d.value == "Greater London"
    assert d.sale_count == 1000


def test_location_type_values() -> None:
    """LocationType enum has expected values."""
    assert LocationType.COUNTY.value == "county"
    assert LocationType.STREET.value == "street"


def test_line_graph_point_stored() -> None:
    """LineGraphPointStored has year_sold and price metrics."""
    p = LineGraphPointStored(year_sold="2021", avg_price=100.0, median_price=95.0, mode_price=90.0, sale_count=10)
    assert p.year_sold == "2021"
    assert p.sale_count == 10


def test_heatmap_cell_stored() -> None:
    """HeatmapCellStored has year_bought, year_sold, and appreciation metrics."""
    c = HeatmapCellStored(year_bought="2019", year_sold="2021", avg_appreciation_pounds=32000, median_appreciation_pounds=31000, sale_count=50, pct_sales_appreciated=88.0)
    assert c.year_bought == "2019"
    assert c.year_sold == "2021"
    assert c.median_appreciation_pounds == 31000
