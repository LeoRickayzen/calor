from __future__ import annotations

import json

from etl.job.etl_helpers import LineGraphRow, line_graph_json_from_list


class TestLineGraphJsonFromList:
    def test_empty(self):
        assert line_graph_json_from_list([]) == "[]"
        assert line_graph_json_from_list(None) == "[]"

    def test_single_point(self):
        row = LineGraphRow(year_sold=2019, avg_price=250000.0, median_price=250000.0, mode_price=250000.0, sale_count=1)
        out = line_graph_json_from_list([row])
        data = json.loads(out)
        assert len(data) == 1
        assert data[0]["year_sold"] == "2019"
        assert data[0]["avg_price"] == 250000.0
        assert data[0]["sale_count"] == 1

    def test_multiple_years(self):
        rows = [
            LineGraphRow(year_sold=2019, avg_price=250000.0, median_price=250000.0, mode_price=250000.0, sale_count=1),
            LineGraphRow(year_sold=2021, avg_price=280000.0, median_price=280000.0, mode_price=280000.0, sale_count=1),
        ]
        out = line_graph_json_from_list(rows)
        data = json.loads(out)
        assert len(data) == 2
        assert data[0]["year_sold"] == "2019" and data[0]["avg_price"] == 250000.0
        assert data[1]["year_sold"] == "2021" and data[1]["avg_price"] == 280000.0

    def test_nulls_become_zero_or_empty(self):
        row = LineGraphRow(year_sold=None, avg_price=None, median_price=None, mode_price=None, sale_count=None)
        out = line_graph_json_from_list([row])
        data = json.loads(out)
        assert data[0]["year_sold"] == ""
        assert data[0]["avg_price"] == 0.0
        assert data[0]["sale_count"] == 0

    def test_rounding(self):
        row = LineGraphRow(year_sold=2019, avg_price=250000.0, median_price=250000.0, mode_price=250000.0, sale_count=1)
        out = line_graph_json_from_list([row])
        data = json.loads(out)
        assert data[0]["avg_price"] == 250000.0
