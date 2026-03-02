"""Constants shared across ETL jobs and helpers."""
from __future__ import annotations

# PPD property type: D=detached, S=semi_detached, T=terraced, F=flat, O=other
PROPERTY_TYPE_MAP: dict[str, str] = {
    "d": "detached",
    "s": "semi_detached",
    "t": "terraced",
    "f": "flat",
    "o": "other",
}

# Duration: F=freehold, L=leasehold
DURATION_MAP: dict[str, str] = {
    "f": "freehold",
    "l": "leasehold",
}

# Permutations for expansion: (use_all_location, use_all_house_type, use_all_tenure); 1 = use "all"
# 2^3 = 8 rows so each sale contributes to exact segment and all coarser aggregations
EXPANSION_MASKS: list[tuple[int, int, int]] = [
    (0, 0, 0),
    (1, 0, 0),
    (0, 1, 0),
    (0, 0, 1),
    (1, 1, 0),
    (1, 0, 1),
    (0, 1, 1),
    (1, 1, 1),
]

