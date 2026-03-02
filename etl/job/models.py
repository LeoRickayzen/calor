"""Dataclasses representing aggregated rows for line graphs and heatmaps."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class LineGraphRow:
    """One row from line path aggregation (year_sold, avg_price, ...)."""

    year_sold: int | None
    avg_price: float | None
    median_price: float | None
    mode_price: float | None
    sale_count: int | None

    @classmethod
    def from_mapping(cls, m: Mapping[str, Any]) -> "LineGraphRow":
        """Build from dict or Spark Row (e.g. from collect_list). Spark uses _year_sold."""

        def get(key: str) -> Any:
            try:
                return m[key]
            except (KeyError, TypeError):
                return None

        return cls(
            year_sold=get("_year_sold") or get("year_sold"),
            avg_price=get("avg_price"),
            median_price=get("median_price"),
            mode_price=get("mode_price"),
            sale_count=get("sale_count"),
        )


@dataclass(frozen=True)
class HeatmapRow:
    """One row from heatmap path aggregation (year_bought, year_sold, appreciation metrics)."""

    year_bought: int | None
    year_sold: int | None
    avg_appreciation_pounds: float | None
    median_appreciation_pounds: float | None
    sale_count: int | None
    avg_appreciation_pct: float | None
    median_appreciation_pct: float | None
    pct_sales_appreciated: float | None
    biggest_loser_pounds: float | None
    biggest_loser_pct: float | None
    biggest_winner_pounds: float | None
    biggest_winner_pct: float | None

    @classmethod
    def from_mapping(cls, m: Mapping[str, Any]) -> "HeatmapRow":
        """Build from dict or Spark Row (e.g. from collect_list). Spark uses _year_bought, _year_sold."""

        def get(key: str) -> Any:
            try:
                return m[key]
            except (KeyError, TypeError):
                return None

        return cls(
            year_bought=get("_year_bought") or get("year_bought"),
            year_sold=get("_year_sold") or get("year_sold"),
            avg_appreciation_pounds=get("avg_appreciation_pounds"),
            median_appreciation_pounds=get("median_appreciation_pounds"),
            sale_count=get("sale_count"),
            avg_appreciation_pct=get("avg_appreciation_pct"),
            median_appreciation_pct=get("median_appreciation_pct"),
            pct_sales_appreciated=get("pct_sales_appreciated"),
            biggest_loser_pounds=get("biggest_loser_pounds"),
            biggest_loser_pct=get("biggest_loser_pct"),
            biggest_winner_pounds=get("biggest_winner_pounds"),
            biggest_winner_pct=get("biggest_winner_pct"),
        )

