"""Pure Python helpers for ETL: normalisation, primary key expansion, line/heatmap JSON. No Spark."""
from __future__ import annotations

import json
import re

from etl.job.constants import DURATION_MAP, EXPANSION_MASKS, PROPERTY_TYPE_MAP
from etl.job.models import HeatmapRow, LineGraphRow


def normalise(text: str) -> str:
    """Lowercase, single space, no #."""
    if not text or text.strip() == "":
        return "unknown"
    text = re.sub(r"#", "", text.strip().lower())
    text = re.sub(r"\s+", " ", text)
    return text


def normalise_property_type(value: str | None) -> str:
    if value is None or value.strip() == "":
        return "other"
    return PROPERTY_TYPE_MAP.get(value.strip().lower()[:1], "other")


def normalise_duration(value: str | None) -> str:
    if value is None or value.strip() == "":
        return "unknown"
    return DURATION_MAP.get(value.strip().lower()[:1], "unknown")


def normalise_postcode_prefix(value: str | None) -> str:
    """UK postcode outcode (part before space), normalised. e.g. 'N15 3EP' -> 'n15'."""
    if value is None:
        return "unknown"
    s = value.strip()
    if not s:
        return "unknown"
    parts = s.split()
    first_token = parts[0] if parts else ""
    return normalise(first_token) if first_token else "unknown"


def primary_keys_for_segment(
    location_type: str,
    location_value: str,
    house_type: str,
    tenure: str,
    size_band: str = "all",
    year_built_band: str = "all",
) -> list[str]:
    """Return the 8 primary_key strings for one segment (same formula as Spark expansion)."""
    keys = []
    for use_all_loc, use_all_house, use_all_tenure in EXPANSION_MASKS:
        loc_val = "all" if use_all_loc else location_value
        ht = "all" if use_all_house else house_type
        ten = "all" if use_all_tenure else tenure
        key = "#".join([location_type, loc_val, ht, ten, size_band, year_built_band])
        keys.append(key)
    return keys


def line_graph_json_from_list(line_list: list[LineGraphRow] | None) -> str:
    """Build line_graph JSON array from list of (year_sold, avg_price, ...) rows."""
    out = []
    for row in line_list or []:
        year_sold = str(row.year_sold) if row.year_sold is not None else ""
        avg_price = float(row.avg_price) if row.avg_price is not None else 0.0
        median_price = float(row.median_price) if row.median_price is not None else 0.0
        mode_price = float(row.mode_price) if row.mode_price is not None else 0.0
        sale_count = int(row.sale_count) if row.sale_count is not None else 0
        out.append({
            "year_sold": year_sold,
            "avg_price": round(avg_price, 2),
            "median_price": round(median_price, 2),
            "mode_price": round(mode_price, 2),
            "sale_count": sale_count,
        })
    return json.dumps(out)


def float_from_row(row: HeatmapRow, key: str) -> float | None:
    """Read a float from a HeatmapRow by attribute name; return None if missing or invalid."""
    try:
        v = getattr(row, key, None)
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def heatmap_graph_json_from_list(heatmap_list: list[HeatmapRow] | None) -> str:
    """Build heatmap_graph JSON array from list of (year_bought, year_sold, appreciation metrics) rows."""
    out = []
    for row in heatmap_list or []:
        year_bought = str(row.year_bought) if row.year_bought is not None else ""
        year_sold = str(row.year_sold) if row.year_sold is not None else ""
        avg_pounds = float(row.avg_appreciation_pounds) if row.avg_appreciation_pounds is not None else 0.0
        median_pounds = float(row.median_appreciation_pounds) if row.median_appreciation_pounds is not None else 0.0
        sale_count = int(row.sale_count) if row.sale_count is not None else 0
        avg_pct = float(row.avg_appreciation_pct) if row.avg_appreciation_pct is not None else None
        median_pct = float(row.median_appreciation_pct) if row.median_appreciation_pct is not None else None
        pct_appreciated = float(row.pct_sales_appreciated) if row.pct_sales_appreciated is not None else 0.0
        loser_pounds = float_from_row(row, "biggest_loser_pounds")
        loser_pct = float_from_row(row, "biggest_loser_pct")
        winner_pounds = float_from_row(row, "biggest_winner_pounds")
        winner_pct = float_from_row(row, "biggest_winner_pct")
        out.append({
            "year_bought": year_bought,
            "year_sold": year_sold,
            "avg_appreciation_pounds": round(avg_pounds, 2),
            "median_appreciation_pounds": round(median_pounds, 2),
            "sale_count": sale_count,
            "avg_appreciation_pct": round(avg_pct, 2) if avg_pct is not None else None,
            "median_appreciation_pct": round(median_pct, 2) if median_pct is not None else None,
            "pct_sales_appreciated": round(pct_appreciated, 1),
            "biggest_loser_pounds": round(loser_pounds, 2) if loser_pounds is not None else None,
            "biggest_loser_pct": round(loser_pct, 2) if loser_pct is not None else None,
            "biggest_winner_pounds": round(winner_pounds, 2) if winner_pounds is not None else None,
            "biggest_winner_pct": round(winner_pct, 2) if winner_pct is not None else None,
        })
    return json.dumps(out)


def combine_line_and_heatmap_to_value(
    line_graph: str, heatmap_graph: str, sale_count: int | None
) -> str:
    """Build stored value JSON: { line_graph, heatmap_graph, sale_count }."""
    heatmap = heatmap_graph if heatmap_graph is not None and heatmap_graph.strip() else "[]"
    return json.dumps({
        "line_graph": json.loads(line_graph) if line_graph else [],
        "heatmap_graph": json.loads(heatmap) if heatmap else [],
        "sale_count": int(sale_count) if sale_count is not None else 0,
    })
