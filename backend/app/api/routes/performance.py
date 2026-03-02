"""Performance endpoints: list, line graph, heatmap."""

from __future__ import annotations

from collections import defaultdict

from fastapi import APIRouter, Depends, Query

from app.db.repository import PerformanceRepository
from app.models.schemas import (
    HeatmapCell,
    HeatmapResponse,
    LineGraphPoint,
    LineGraphResponse,
    LocationType,
    PerformanceItemResponse,
    PerformanceListResponse,
)

router = APIRouter()

def get_repo() -> PerformanceRepository:
    return PerformanceRepository()


@router.get("", response_model=PerformanceListResponse)
def list_performance(
    location_type: LocationType = Query(...),
    location_value: str = Query(..., min_length=1),
    house_type: str = Query("all", description="House type; use values from GET /api/dimensions/house_type or 'all'"),
    tenure: str = Query("all", description="Tenure; use values from GET /api/dimensions/tenure or 'all'"),
    size_band: str = Query("all", description="Size band; use values from GET /api/dimensions/size_band or 'all'"),
    year_built_band: str = Query("all", description="Year built band; use values from GET /api/dimensions/year_built_band or 'all'"),
    repo: PerformanceRepository = Depends(get_repo),
) -> PerformanceListResponse:
    """Return performance for a location and dimensions. Use 'all' for any dimension to use the aggregate (stored in pk)."""
    items = repo.query_by_location(
        location_type,
        location_value,
        house_type=house_type,
        tenure=tenure,
        size_band=size_band,
        year_built_band=year_built_band,
    )
    return PerformanceListResponse(
        items=[
            PerformanceItemResponse(
                line_graph=i.line_graph,
                heatmap_graph=i.heatmap_graph,
                sale_count=i.sale_count,
            )
            for i in items
        ],
        count=len(items),
    )


@router.get("/line", response_model=LineGraphResponse)
def line_graph(
    location_type: LocationType = Query(...),
    location_value: str = Query(..., min_length=1),
    house_type: str = Query("all", description="House type; use values from GET /api/dimensions/house_type or 'all'"),
    tenure: str = Query("all", description="Tenure; use values from GET /api/dimensions/tenure or 'all'"),
    size_band: str = Query("all", description="Size band; use values from GET /api/dimensions/size_band or 'all'"),
    year_built_band: str = Query("all", description="Year built band; use values from GET /api/dimensions/year_built_band or 'all'"),
    repo: PerformanceRepository = Depends(get_repo),
) -> LineGraphResponse:
    """Return line graph by year of sale. Use 'all' for any dimension to use the aggregate (stored in pk)."""
    items = repo.query_by_location(
        location_type,
        location_value,
        house_type=house_type,
        tenure=tenure,
        size_band=size_band,
        year_built_band=year_built_band,
    )
    if not items:
        return LineGraphResponse(series=[])

    if len(items) == 1:
        series = [
            LineGraphPoint(
                year_sold=p.year_sold,
                avg_price=p.avg_price,
                median_price=p.median_price,
                mode_price=p.mode_price,
                sale_count=p.sale_count,
            )
            for p in items[0].line_graph
        ]
        return LineGraphResponse(series=series)

    # Merge by year_sold: weighted average for prices, sum sale_count
    by_year: dict[str, list[tuple[float, float, float, int]]] = defaultdict(list)
    for i in items:
        for p in i.line_graph:
            by_year[p.year_sold].append((p.avg_price, p.median_price, p.mode_price, p.sale_count))

    series = []
    for year_sold in sorted(by_year.keys()):
        rows = by_year[year_sold]
        total_count = sum(r[3] for r in rows)
        if total_count == 0:
            avg_price = sum(r[0] for r in rows) / len(rows)
            median_price = sum(r[1] for r in rows) / len(rows)
            mode_price = sum(r[2] for r in rows) / len(rows)
        else:
            avg_price = sum(r[0] * r[3] for r in rows) / total_count
            median_price = sum(r[1] * r[3] for r in rows) / total_count
            mode_price = sum(r[2] * r[3] for r in rows) / total_count
        series.append(
            LineGraphPoint(
                year_sold=year_sold,
                avg_price=avg_price,
                median_price=median_price,
                mode_price=mode_price,
                sale_count=total_count,
            )
        )
    return LineGraphResponse(series=series)


@router.get("/heatmap", response_model=HeatmapResponse)
def heatmap(
    location_type: LocationType = Query(...),
    location_value: str = Query(..., min_length=1),
    house_type: str = Query("all", description="House type; use values from GET /api/dimensions/house_type or 'all'"),
    tenure: str = Query("all", description="Tenure; use values from GET /api/dimensions/tenure or 'all'"),
    size_band: str = Query("all", description="Size band; use values from GET /api/dimensions/size_band or 'all'"),
    year_built_band: str = Query("all", description="Year built band; use values from GET /api/dimensions/year_built_band or 'all'"),
    repo: PerformanceRepository = Depends(get_repo),
) -> HeatmapResponse:
    """Return heatmap cells (year_bought × year_sold) with full metrics. Use 'all' for any dimension to use the aggregate (stored in pk)."""
    items = repo.query_by_location(
        location_type,
        location_value,
        house_type=house_type,
        tenure=tenure,
        size_band=size_band,
        year_built_band=year_built_band,
    )
    if not items:
        return HeatmapResponse(cells=[])

    if len(items) == 1:
        cells = [
            HeatmapCell(
                year_bought=c.year_bought,
                year_sold=c.year_sold,
                avg_appreciation_pounds=c.avg_appreciation_pounds,
                median_appreciation_pounds=c.median_appreciation_pounds,
                sale_count=c.sale_count,
                avg_appreciation_pct=c.avg_appreciation_pct,
                median_appreciation_pct=c.median_appreciation_pct,
                pct_sales_appreciated=c.pct_sales_appreciated,
            )
            for c in items[0].heatmap_graph
        ]
        return HeatmapResponse(cells=cells)

    # Merge by (year_bought, year_sold): weighted average by sale_count
    key_to_rows: dict[tuple[str, str], list] = defaultdict(list)
    for i in items:
        for c in i.heatmap_graph:
            key_to_rows[(c.year_bought, c.year_sold)].append(c)

    cells = []
    for (year_bought, year_sold), rows in sorted(key_to_rows.items()):
        total_count = sum(r.sale_count for r in rows)
        if total_count == 0:
            avg_pounds = sum(r.avg_appreciation_pounds for r in rows) / len(rows)
            median_pounds = sum(r.median_appreciation_pounds for r in rows) / len(rows)
            avg_pct = sum(r.avg_appreciation_pct or 0 for r in rows) / len(rows)
            median_pct = sum(r.median_appreciation_pct or 0 for r in rows) / len(rows)
            pct_app = sum(r.pct_sales_appreciated for r in rows) / len(rows)
        else:
            avg_pounds = sum(r.avg_appreciation_pounds * r.sale_count for r in rows) / total_count
            median_pounds = sum(r.median_appreciation_pounds * r.sale_count for r in rows) / total_count
            pct_app = sum(r.pct_sales_appreciated * r.sale_count for r in rows) / total_count
            avg_pct_pairs = [(r.avg_appreciation_pct, r.sale_count) for r in rows if r.avg_appreciation_pct is not None]
            median_pct_pairs = [(r.median_appreciation_pct, r.sale_count) for r in rows if r.median_appreciation_pct is not None]
            avg_pct = (sum(v * sc for v, sc in avg_pct_pairs) / sum(sc for _, sc in avg_pct_pairs)) if avg_pct_pairs else None
            median_pct = (sum(v * sc for v, sc in median_pct_pairs) / sum(sc for _, sc in median_pct_pairs)) if median_pct_pairs else None

        cells.append(
            HeatmapCell(
                year_bought=year_bought,
                year_sold=year_sold,
                avg_appreciation_pounds=avg_pounds,
                median_appreciation_pounds=median_pounds,
                sale_count=total_count,
                avg_appreciation_pct=avg_pct,
                median_appreciation_pct=median_pct,
                pct_sales_appreciated=pct_app,
            )
        )
    return HeatmapResponse(cells=cells)
