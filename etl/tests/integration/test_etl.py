"""ETL test: run job on fixtures/input.csv, compare house_price_performance output to fixtures/expected_output.csv (hand-written)."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).resolve().parent.parent
ETL_ROOT = TESTS_DIR.parent
REPO_ROOT = ETL_ROOT.parent
ETL_MODULE = "etl.job.etl"

FIXTURES_DIR = TESTS_DIR / "fixtures"
INPUT_CSV = FIXTURES_DIR / "input.csv"
EXPECTED_OUTPUT_CSV = FIXTURES_DIR / "expected_output.csv"


def _run_etl(output_dir: Path) -> None:
    env = {**os.environ, "ETL_INPUT_PATH": str(INPUT_CSV.resolve()), "ETL_OUTPUT_PATH": str(output_dir.resolve())}
    result = subprocess.run(
        [sys.executable, "-m", ETL_MODULE],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"ETL failed: {result.stderr!r}"


def _read_key_value_csv(path: Path) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    if len(lines) < 2:
        return rows
    for line in lines[1:]:
        line = line.rstrip("\n")
        first_comma = line.find(",")
        if first_comma < 0:
            continue
        key = line[:first_comma].strip().strip('"')
        value = line[first_comma + 1 :].strip()
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1].replace('\\"', '"')
        rows.append((key, value))
    return rows


def _load_actual_output(output_dir: Path) -> list[tuple[str, str]]:
    perf_dir = output_dir / "house_price_performance"
    all_rows: list[tuple[str, str]] = []
    for p in sorted(perf_dir.glob("*.csv")):
        all_rows.extend(_read_key_value_csv(p))
    return sorted(all_rows, key=lambda r: r[0])


def _load_expected(path: Path) -> list[tuple[str, str]]:
    return sorted(_read_key_value_csv(path), key=lambda r: r[0])


def test_etl_output_matches_expected(tmp_path: Path) -> None:
    assert INPUT_CSV.exists(), f"Missing fixture {INPUT_CSV}"

    _run_etl(tmp_path)
    actual = _load_actual_output(tmp_path)
    assert actual, "ETL produced no house_price_performance rows"

    assert EXPECTED_OUTPUT_CSV.exists(), f"Missing fixture {EXPECTED_OUTPUT_CSV}"
    expected = _load_expected(EXPECTED_OUTPUT_CSV)

    assert len(actual) == len(expected), (
        f"Row count mismatch: actual {len(actual)}, expected {len(expected)}"
    )
    for (ak, av), (ek, ev) in zip(actual, expected):
        assert ak == ek, f"Key mismatch: {ak!r} vs {ek!r}"
        assert av == ev, f"Value mismatch for key {ak!r}"
