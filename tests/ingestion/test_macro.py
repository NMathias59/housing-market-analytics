"""Unit tests for include/ingestion/scripts/macro.py"""

from __future__ import annotations

import os
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from include.ingestion.scripts.macro import (
    SOURCE, TABLE,
    parse, run,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_chunk(**overrides) -> pd.DataFrame:
    data = {
        "date": ["2024-11-01"],
        "taux_credit_immobilier": ["3,75"],
        "taux_oat_10ans": ["3,20"],
        "duree_moyenne_credit": ["21,5"],
        "taux_inflation": ["1,8"],
    }
    data.update(overrides)
    return pd.DataFrame(data)


def _run_mocked(
    *,
    watermark: str | None = None,
    chunks: list | None = None,
    full_refresh: bool = False,
    dry_run: bool = False,
):
    if chunks is None:
        chunks = [_make_chunk()]

    client = MagicMock()
    mocks: dict = {"client": client}

    with ExitStack() as stack:
        stack.enter_context(
            patch.dict(os.environ, {"MACRO_BDF_URL": "http://fake-bdf/taux.csv"})
        )
        stack.enter_context(
            patch("include.ingestion.scripts.macro.get_client", return_value=client)
        )
        stack.enter_context(patch("include.ingestion.scripts.macro.ensure_watermark_table"))
        mocks["get_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.macro.get_watermark", return_value=watermark)
        )
        mocks["iter_csv_chunks"] = stack.enter_context(
            patch("include.ingestion.scripts.macro.iter_csv_chunks",
                  return_value=iter(chunks))
        )
        mocks["load_df"] = stack.enter_context(
            patch("include.ingestion.scripts.macro.load_df",
                  side_effect=lambda c, df, t: len(df))
        )
        mocks["set_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.macro.set_watermark")
        )
        result = run(full_refresh=full_refresh, dry_run=dry_run)

    return result, mocks


# ---------------------------------------------------------------------------
# parse
# ---------------------------------------------------------------------------

class TestParse:
    def test_returns_dataframe(self):
        assert isinstance(parse(_make_chunk()), pd.DataFrame)

    def test_parses_date(self):
        df = parse(_make_chunk(date=["2024-11-01"]))
        assert pd.api.types.is_datetime64_any_dtype(df["date"])

    def test_derives_annee_from_date(self):
        df = parse(_make_chunk(date=["2024-03-01"]))
        assert df["annee"].iloc[0] == 2024

    def test_derives_mois_from_date(self):
        df = parse(_make_chunk(date=["2024-03-01"]))
        assert df["mois"].iloc[0] == 3

    def test_converts_french_decimal_comma(self):
        df = parse(_make_chunk(taux_credit_immobilier=["3,75"]))
        assert df["taux_credit_immobilier"].iloc[0] == pytest.approx(3.75)

    def test_invalid_taux_becomes_nan(self):
        df = parse(_make_chunk(taux_oat_10ans=["N/D"]))
        assert pd.isna(df["taux_oat_10ans"].iloc[0])

    def test_since_date_filters_older_rows(self):
        chunk = pd.DataFrame({
            "date": ["2024-06-01", "2024-12-01"],
            "taux_credit_immobilier": ["3,5", "3,8"],
        })
        df = parse(chunk, since_date="2024-11")
        assert len(df) == 1
        assert df["date"].iloc[0] == pd.Timestamp("2024-12-01")

    def test_no_since_date_returns_all_rows(self):
        chunk = pd.DataFrame({
            "date": ["2024-01-01", "2024-06-01", "2024-12-01"],
            "taux_credit_immobilier": ["3,5", "3,6", "3,8"],
        })
        df = parse(chunk, since_date=None)
        assert len(df) == 3

    def test_invalid_date_rows_dropped(self):
        chunk = pd.DataFrame({
            "date": ["not-a-date", "2024-11-01"],
            "taux_credit_immobilier": ["3,5", "3,75"],
        })
        df = parse(chunk)
        assert len(df) == 1

    def test_unknown_columns_ignored(self):
        chunk = _make_chunk()
        chunk["champ_inattendu"] = "x"
        df = parse(chunk)
        assert "champ_inattendu" not in df.columns

    def test_does_not_mutate_input(self):
        chunk = _make_chunk()
        cols = list(chunk.columns)
        parse(chunk)
        assert list(chunk.columns) == cols


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

class TestRun:
    def test_creates_table_on_startup(self):
        _, mocks = _run_mocked(watermark="2024-10")
        sql = mocks["client"].command.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert TABLE in sql

    def test_raises_if_url_not_set(self):
        env = {k: v for k, v in os.environ.items() if k != "MACRO_BDF_URL"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(EnvironmentError, match="MACRO_BDF_URL"):
                run()

    def test_full_refresh_skips_watermark_read(self):
        _, mocks = _run_mocked(watermark="2024-10", full_refresh=True)
        mocks["get_watermark"].assert_not_called()

    def test_incremental_passes_watermark_to_parse(self):
        """Watermark is passed through to iter_csv_chunks / parse logic."""
        _, mocks = _run_mocked(watermark="2024-10")
        mocks["get_watermark"].assert_called_once()

    def test_dry_run_skips_insert_and_watermark(self):
        result, mocks = _run_mocked(watermark="2024-10", dry_run=True)
        assert result == 0
        mocks["load_df"].assert_not_called()
        mocks["set_watermark"].assert_not_called()

    def test_watermark_set_to_max_date(self):
        chunk = pd.DataFrame({
            "date": ["2024-10-01", "2024-11-01"],
            "taux_credit_immobilier": ["3,6", "3,75"],
        })
        _, mocks = _run_mocked(watermark=None, chunks=[chunk])
        called_value = mocks["set_watermark"].call_args[0][2]
        assert called_value == "2024-11"

    def test_returns_row_count(self):
        result, _ = _run_mocked(watermark="2024-10")
        assert result == 1

    def test_empty_after_filter_skips_insert(self):
        """All rows older than watermark → nothing inserted, watermark unchanged."""
        chunk = pd.DataFrame({
            "date": ["2024-09-01"],
            "taux_credit_immobilier": ["3,5"],
        })
        result, mocks = _run_mocked(watermark="2024-10", chunks=[chunk])
        assert result == 0
        mocks["load_df"].assert_not_called()
        mocks["set_watermark"].assert_not_called()
