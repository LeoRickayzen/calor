"""Zlib compression for house_price_performance graph data (line_graph, heatmap_graph) in DynamoDB."""

from __future__ import annotations

import base64
import json
import zlib
from decimal import Decimal
from typing import Any


def _json_default(obj: Any) -> Any:
    """JSON serializer for Decimal."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def compress_graph_data(data: list[Any]) -> str:
    """Compress a list of dicts (line_graph or heatmap_graph) to a base64 string for DynamoDB."""
    if not data:
        return _compress(b"[]")
    return _compress(
        json.dumps(data, default=_json_default).encode("utf-8"),
    )


def _compress(raw: bytes) -> str:
    return base64.b64encode(zlib.compress(raw)).decode("ascii")


def decompress_graph_data(s: str) -> list[Any]:
    """Decompress a base64+zlib string from DynamoDB to a list of dicts."""
    if not s or not isinstance(s, str):
        return []
    try:
        raw = base64.b64decode(s.encode("ascii"))
        return json.loads(zlib.decompress(raw).decode("utf-8"))
    except (ValueError, zlib.error, TypeError):
        return []
