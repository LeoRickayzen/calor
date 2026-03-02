from __future__ import annotations

import json

from etl.job.etl_helpers import HeatmapRow, float_from_row, heatmap_graph_json_from_list


class TestFloatFromRow:
    def test_missing_key_returns_none(self):
        row = HeatmapRow.from_mapping({})
        assert float_from_row(row, "biggest_winner_pounds") is None

    def test_none_value_returns_none(self):
        row = HeatmapRow.from_mapping({"biggest_winner_pounds": None})
        assert float_from_row(row, "biggest_winner_pounds") is None

    def test_valid_returns_float(self):
        row = HeatmapRow.from_mapping({"biggest_winner_pounds": 10.5})
        assert float_from_row(row, "biggest_winner_pounds") == 10.5
        row2 = HeatmapRow.from_mapping({"biggest_winner_pounds": 10})
        assert float_from_row(row2, "biggest_winner_pounds") == 10.0


class TestHeatmapGraphJsonFromList:
    def test_empty(self):
        assert heatmap_graph_json_from_list([]) == "[]"
        assert heatmap_graph_json_from_list(None) == "[]"

    def test_one_cell(self):
        row = HeatmapRow(
            year_bought=2019, year_sold=2021,
            avg_appreciation_pounds=30000.0, median_appreciation_pounds=30000.0, sale_count=1,
            avg_appreciation_pct=12.0, median_appreciation_pct=12.0, pct_sales_appreciated=100.0,
            biggest_loser_pounds=30000.0, biggest_loser_pct=12.0,
            biggest_winner_pounds=30000.0, biggest_winner_pct=12.0,
        )
        out = heatmap_graph_json_from_list([row])
        data = json.loads(out)
        assert len(data) == 1
        assert data[0]["year_bought"] == "2019" and data[0]["year_sold"] == "2021"
        assert data[0]["avg_appreciation_pounds"] == 30000.0
        assert data[0]["pct_sales_appreciated"] == 100.0

    def test_multiple_cells(self):
        rows = [
            HeatmapRow(year_bought=2019, year_sold=2021, avg_appreciation_pounds=100.0, median_appreciation_pounds=100.0, sale_count=1, avg_appreciation_pct=1.0, median_appreciation_pct=1.0, pct_sales_appreciated=100.0, biggest_loser_pounds=100.0, biggest_loser_pct=1.0, biggest_winner_pounds=100.0, biggest_winner_pct=1.0),
            HeatmapRow(year_bought=2020, year_sold=2022, avg_appreciation_pounds=200.0, median_appreciation_pounds=200.0, sale_count=1, avg_appreciation_pct=2.0, median_appreciation_pct=2.0, pct_sales_appreciated=100.0, biggest_loser_pounds=200.0, biggest_loser_pct=2.0, biggest_winner_pounds=200.0, biggest_winner_pct=2.0),
        ]
        out = heatmap_graph_json_from_list(rows)
        data = json.loads(out)
        assert len(data) == 2
