"""Repository for querying house_price_performance and dimension_index."""

from __future__ import annotations

from typing import Any

from app.config import settings
from app.db.client import get_dynamo_resource
from app.models.schemas import HousePricePerformanceItem, LocationType


class PerformanceRepository:
    """Query pre-aggregated house price performance and dimension index."""

    def __init__(self, resource: Any | None = None) -> None:
        self._resource = resource or get_dynamo_resource()
        self._table = self._resource.Table(settings.table_house_price_performance)
        self._dimension_table = self._resource.Table(settings.table_dimension_index)

    def _normalise_location_value(self, location_type: LocationType, location_value: str) -> str:
        """ETL stores location_value in lowercase for county/postcode/borough; normalise so lookups match."""
        if location_type in (LocationType.COUNTY, LocationType.POSTCODE, LocationType.BOROUGH):
            return location_value.strip().lower()
        return location_value.strip()

    def _pk(
        self,
        location_type: LocationType,
        location_value: str,
        house_type: str,
        tenure: str,
        size_band: str,
        year_built_band: str,
    ) -> str:
        """Full partition key: location_type#location_value#house_type#tenure#size_band#year_built_band. Segment values normalised to match ETL (lowercase)."""
        loc_val = self._normalise_location_value(location_type, location_value)
        ht = (house_type or "all").strip().lower()
        tn = (tenure or "all").strip().lower()
        sb = (size_band or "all").strip().lower()
        ybb = (year_built_band or "all").strip().lower()
        return f"{location_type.value}#{loc_val}#{ht}#{tn}#{sb}#{ybb}"

    def query_by_location(
        self,
        location_type: LocationType,
        location_value: str,
        *,
        house_type: str,
        tenure: str,
        size_band: str,
        year_built_band: str,
    ) -> list[HousePricePerformanceItem]:
        """
        Build key from filters and GetItem. Dimension filters use 'all' for aggregate (caller passes it).
        """
        pk = self._pk(location_type, location_value, house_type, tenure, size_band, year_built_band)
        response = self._table.get_item(Key={"pk": pk})
        if "Item" in response:
            return [HousePricePerformanceItem.from_dynamo(response["Item"])]
        return []

    def get_dimension_values(self, dimension_name: str) -> list[dict[str, Any]]:
        """Return all dimension values for a given dimension (e.g. county, house_type)."""
        pk = f"meta#{dimension_name}"
        response = self._dimension_table.query(
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": pk},
        )
        items = list(response.get("Items", []))
        while response.get("LastEvaluatedKey"):
            response = self._dimension_table.query(
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": pk},
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            items.extend(response.get("Items", []))
        return items
