"""Unit tests for graph data compression (zlib + base64)."""

import pytest

from app.db.compression import compress_graph_data, decompress_graph_data


def test_compress_decompress_roundtrip() -> None:
    """Compressing then decompressing returns the original list."""
    data = [
        {"year_sold": "2020", "avg_price": 480000, "sale_count": 142},
        {"year_sold": "2021", "avg_price": 510000, "sale_count": 158},
    ]
    compressed = compress_graph_data(data)
    assert isinstance(compressed, str)
    assert len(compressed) > 0
    decompressed = decompress_graph_data(compressed)
    assert decompressed == data


def test_compress_empty_list() -> None:
    """Empty list compresses to a short base64 string; decompress returns []."""
    compressed = compress_graph_data([])
    assert isinstance(compressed, str)
    decompressed = decompress_graph_data(compressed)
    assert decompressed == []


def test_decompress_invalid_returns_empty_list() -> None:
    """Invalid or empty input to decompress_graph_data returns []."""
    assert decompress_graph_data("") == []
    assert decompress_graph_data("not-valid-base64!!!") == []
