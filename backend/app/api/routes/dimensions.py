"""Dimension index endpoints for filter dropdowns."""

from fastapi import APIRouter, Depends

from app.db.repository import PerformanceRepository
from app.models.schemas import DimensionIndexItem, DimensionListResponse

router = APIRouter()


def get_repo() -> PerformanceRepository:
    return PerformanceRepository()


@router.get("/{dimension_name}", response_model=DimensionListResponse)
def list_dimension_values(
    dimension_name: str,
    repo: PerformanceRepository = Depends(get_repo),
) -> DimensionListResponse:
    """Return all values for a dimension (e.g. county, house_type) for filter dropdowns. Values come from dimension_index (include 'all' in DB for no-filter option)."""
    raw = repo.get_dimension_values(dimension_name)
    values = []
    for item in raw:
        sc = item.get("sale_count")
        sale_count = int(sc) if sc is not None else None
        label_val = item.get("label")
        label = str(label_val) if label_val is not None else None
        values.append(
            DimensionIndexItem(
                dimension_name=dimension_name,
                value=str(item.get("sk", "")),
                label=label,
                sale_count=sale_count,
            )
        )
    return DimensionListResponse(dimension_name=dimension_name, values=values)
