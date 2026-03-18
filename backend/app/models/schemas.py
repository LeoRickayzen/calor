"""Pydantic models for API and DynamoDB item shapes."""

from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field

from app.db.compression import decompress_graph_data


class LocationType(str, Enum):
    """Location filter type."""

    COUNTY = "county"
    POSTCODE = "postcode"
    BOROUGH = "borough"
    STREET = "street"


class DimensionType(str, Enum):
    """Dimension index type for filter dropdowns."""

    COUNTY = "county"
    BOROUGH = "borough"
    POSTCODE = "postcode"
    HOUSE_TYPE = "house_type"
    TENURE = "tenure"
    SIZE_BAND = "size_band"
    YEAR_BUILT_BAND = "year_built_band"
    YEAR_BOUGHT_BAND = "year_bought_band"


# Filter values (house_type, tenure, size_band, year_built_band) are not enums:
# they are loaded from dimension_index and passed as strings. Use "all" for aggregate.

# --- Helpers for DynamoDB Number -> Python ---


def _num(x: Any) -> float:
    if x is None:
        return 0.0
    if isinstance(x, Decimal):
        return float(x)
    return float(x)


def _int(x: Any) -> int:
    if x is None:
        return 0
    if isinstance(x, Decimal):
        return int(x)
    return int(x)


# --- Stored JSON shapes (line_graph / heatmap_graph in DynamoDB) ---


class LineGraphPointStored(BaseModel):
    """One point in the stored line_graph (by year of sale)."""

    year_sold: str
    avg_price: float
    median_price: float
    mode_price: float
    sale_count: int = 0

    @classmethod
    def from_dynamo(cls, raw: dict[str, Any]) -> "LineGraphPointStored":
        """Build from one element of the line_graph list (DynamoDB list/map shape)."""
        return cls(
            year_sold=str(raw.get("year_sold", "")),
            avg_price=_num(raw.get("avg_price")),
            median_price=_num(raw.get("median_price")),
            mode_price=_num(raw.get("mode_price")),
            sale_count=_int(raw.get("sale_count", 0)),
        )


class HeatmapCellStored(BaseModel):
    """One cell in the stored heatmap_graph (year_bought × year_sold); appreciation metrics."""

    year_bought: str
    year_sold: str
    avg_appreciation_pounds: float = 0.0
    median_appreciation_pounds: float = 0.0
    sale_count: int = 0
    avg_appreciation_pct: Optional[float] = None
    median_appreciation_pct: Optional[float] = None
    pct_sales_appreciated: float = 0.0  # % of sales in this cell that appreciated (sold > bought)

    @classmethod
    def from_dynamo(cls, raw: dict[str, Any]) -> "HeatmapCellStored":
        """Build from one element of the heatmap_graph list (DynamoDB list/map shape)."""
        avg_pct = raw.get("avg_appreciation_pct")
        median_pct = raw.get("median_appreciation_pct")
        return cls(
            year_bought=str(raw.get("year_bought", "")),
            year_sold=str(raw.get("year_sold", "")),
            avg_appreciation_pounds=_num(raw.get("avg_appreciation_pounds")),
            median_appreciation_pounds=_num(raw.get("median_appreciation_pounds")),
            sale_count=_int(raw.get("sale_count", 0)),
            avg_appreciation_pct=float(avg_pct) if avg_pct is not None else None,
            median_appreciation_pct=float(median_pct) if median_pct is not None else None,
            pct_sales_appreciated=_num(raw.get("pct_sales_appreciated")),
        )


# --- DynamoDB item models (stored shape) ---


class HousePricePerformanceItem(BaseModel):
    """Stored value for one house_price_performance item (no key/filter fields)."""

    line_graph: list[LineGraphPointStored]
    heatmap_graph: list[HeatmapCellStored]
    sale_count: Optional[int] = None

    @classmethod
    def from_dynamo(cls, item: dict[str, Any]) -> "HousePricePerformanceItem":
        """Build from DynamoDB item value attributes only (line_graph, heatmap_graph, sale_count). line_graph and heatmap_graph are zlib-compressed base64 strings."""
        line_raw = decompress_graph_data(item["line_graph"])
        line_graph = [LineGraphPointStored.from_dynamo(p) for p in line_raw]

        heat_raw = decompress_graph_data(item["heatmap_graph"])
        heatmap_graph = [HeatmapCellStored.from_dynamo(c) for c in heat_raw]

        sale_count = item.get("sale_count")
        if sale_count is not None:
            sale_count = _int(sale_count)

        return cls(
            line_graph=line_graph,
            heatmap_graph=heatmap_graph,
            sale_count=sale_count,
        )


class DimensionIndexItem(BaseModel):
    """One row in dimension_index (filter option)."""

    dimension_name: str
    value: str
    label: Optional[str] = None
    sale_count: Optional[int] = None


# --- Query params ---


class PerformanceFilterParams(BaseModel):
    """Query params for performance endpoints."""

    location_type: LocationType
    location_value: str = Field(..., min_length=1)
    house_type: Optional[str] = None
    tenure: Optional[str] = None
    size_band: Optional[str] = None
    year_built_band: Optional[str] = None


# --- API response models ---


class PerformanceItemResponse(BaseModel):
    """One performance item returned by the API (value only; filters are in the request)."""

    line_graph: list[LineGraphPointStored]
    heatmap_graph: list[HeatmapCellStored]
    sale_count: Optional[int] = None


class PerformanceListResponse(BaseModel):
    """List of performance items (raw query result)."""

    items: list[PerformanceItemResponse]
    count: int


class LineGraphPoint(BaseModel):
    """One point on the line graph (year of sale + metrics)."""

    year_sold: str
    avg_price: Optional[float] = None
    median_price: Optional[float] = None
    mode_price: Optional[float] = None
    sale_count: int = 0


class LineGraphResponse(BaseModel):
    """Line graph data: time-ordered points by year_sold."""

    series: list[LineGraphPoint]


class HeatmapCell(BaseModel):
    """One cell in the heatmap (year_bought × year_sold) with appreciation metrics."""

    year_bought: str
    year_sold: str
    avg_appreciation_pounds: float = 0.0
    median_appreciation_pounds: float = 0.0
    sale_count: int = 0
    avg_appreciation_pct: Optional[float] = None
    median_appreciation_pct: Optional[float] = None
    pct_sales_appreciated: float = 0.0


class HeatmapResponse(BaseModel):
    """Heatmap data: array of cells (stored heatmap_graph shape)."""

    cells: list[HeatmapCell]


class DimensionListResponse(BaseModel):
    """List of dimension values for filter dropdowns."""

    dimension_name: str
    values: list[DimensionIndexItem]
