# ETL tests

- **fixtures/input.csv**: Minimal Land Registry PPD-style CSV (two rows, same property, two sales).
- **fixtures/expected_output.csv**: Hand-written expected `house_price_performance` output (key, value rows).

## Layout

- **unit/** – Pure Python helper tests (no Spark). One file per suite: `test_normalise.py`, `test_primary_keys.py`, `test_line_graph.py`, `test_heatmap.py`, `test_combine_value.py`.
- **spark/** – Spark aggregation tests: line path, heatmap path, performance join, dimension index. Require a local JVM (PySpark).
- **integration/** – Integration test: run ETL on fixtures, compare output to expected CSV (`test_etl.py`).
- **run_all_tests.sh** – Run all tests (unit + spark + integration). Execute from anywhere; uses repo root.

## Run tests

From repo root:

```bash
./etl/tests/run_all_tests.sh
```

Or with pytest directly (from repo root): `PYTHONPATH=. python -m pytest etl/tests/ -v`

Unit only (no Spark): `PYTHONPATH=. python -m pytest etl/tests/unit/ -v`

Spark aggregation tests only: `PYTHONPATH=. python -m pytest etl/tests/spark/ -v`
