"""Unit tests: combine_line_and_heatmap_to_value."""
from __future__ import annotations

import json

from etl.job.etl_helpers import combine_line_and_heatmap_to_value


class TestCombineLineAndHeatmapToValue:
    def test_normal(self):
        out = combine_line_and_heatmap_to_value('[{"year_sold":"2019"}]', '[{"year_bought":"2019","year_sold":"2021"}]', 2)
        data = json.loads(out)
        assert data["line_graph"] == [{"year_sold": "2019"}]
        assert len(data["heatmap_graph"]) == 1
        assert data["sale_count"] == 2

    def test_empty_line_graph(self):
        out = combine_line_and_heatmap_to_value("[]", "[]", 0)
        data = json.loads(out)
        assert data["line_graph"] == []
        assert data["heatmap_graph"] == []
        assert data["sale_count"] == 0

    def test_empty_heatmap_stored_as_empty_array(self):
        out = combine_line_and_heatmap_to_value("[]", None, 0)
        data = json.loads(out)
        assert data["heatmap_graph"] == []
        out2 = combine_line_and_heatmap_to_value("[]", "", 0)
        data2 = json.loads(out2)
        assert data2["heatmap_graph"] == []

    def test_sale_count_none_becomes_zero(self):
        out = combine_line_and_heatmap_to_value("[]", "[]", None)
        data = json.loads(out)
        assert data["sale_count"] == 0
