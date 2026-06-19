"""Unit tests for include/ingestion/scripts/rpls.py"""

from __future__ import annotations

import os
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from include.ingestion.scripts.rpls import SOURCE, TABLE, build_params, parse, run

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_RECORDS = [
    {
        "annee": "2022",
        "code_commune": "75056",
        "code_departement": "75",
        "code_region": "11",
        "financement": "PLAI",
        "classe_energie": "D",
        "annee_construction": "1975",
        "loyer": "7.5",
        "surface": "48.0",
        "taux_occupation": "0.92",
    },
    {
        "annee": "2023",
        "code_commune": "69123",
        "code_departement": "69",
        "code_region": "84",
        "financement": "PLUS",
        "classe_energie": "C",
        "annee_construction": "2005",
        "loyer": "6.8",
        "surface": "62.0",
        "taux_occupation": "0.88",
    },
]


def _run_mocked(
    *,
    watermark: str | None = None,
    records: list | None = None,
    full_refresh: bool = False,
    dry_run: bool = False,
    rid: str = "fake-rpls-rid",
):
    if records is None:
        records = SAMPLE_RECORDS
    client = MagicMock()
    mocks: dict = {"client": client}

    with ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, {"RPLS_DIDO_RID": rid}))
        stack.enter_context(
            patch("include.ingestion.scripts.rpls.get_client", return_value=client)
        )
        stack.enter_context(patch("include.ingestion.scripts.rpls.ensure_watermark_table"))
        mocks["get_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.rpls.get_watermark", return_value=watermark)
        )
        mocks["fetch_dido_pages"] = stack.enter_context(
            patch("include.ingestion.scripts.rpls.fetch_dido_pages",
                  return_value=iter([records]))
        )
        mocks["load_df"] = stack.enter_context(
            patch("include.ingestion.scripts.rpls.load_df", return_value=len(records))
        )
        mocks["set_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.rpls.set_watermark")
        )
        result = run(full_refresh=full_refresh, dry_run=dry_run)

    return result, mocks


# ---------------------------------------------------------------------------
# build_params
# ---------------------------------------------------------------------------

class TestBuildParams:
    def test_no_watermark_returns_empty(self):
        assert build_params(None) == {}

    def test_with_watermark_adds_gt_filter(self):
        assert build_params(2021) == {"filters": "annee:gt:2021"}


# ---------------------------------------------------------------------------
# parse
# ---------------------------------------------------------------------------

class TestParse:
    def _record(self, **kw):
        base = {
            "annee": "2022",
            "code_commune": "33063", "code_departement": "33", "code_region": "75",
            "financement": "PLS", "classe_energie": "B",
            "annee_construction": "1990",
            "loyer": "8.2", "surface": "55.0", "taux_occupation": "0.95",
        }
        base.update(kw)
        return base

    def test_returns_dataframe(self):
        assert isinstance(parse([self._record()]), pd.DataFrame)

    def test_empty_input_returns_empty_df(self):
        assert parse([]).empty

    def test_parses_annee(self):
        assert parse([self._record(annee="2023")])["annee"].iloc[0] == 2023

    def test_parses_annee_construction_as_int(self):
        df = parse([self._record(annee_construction="1985")])
        assert df["annee_construction"].iloc[0] == 1985

    def test_parses_loyer_as_float(self):
        df = parse([self._record(loyer="9.75")])
        assert df["loyer"].iloc[0] == pytest.approx(9.75)

    def test_invalid_loyer_becomes_nan(self):
        df = parse([self._record(loyer="N/D")])
        assert pd.isna(df["loyer"].iloc[0])

    def test_null_financement_becomes_empty(self):
        df = parse([self._record(financement=None)])
        assert df["financement"].iloc[0] == ""

    def test_unknown_columns_dropped(self):
        df = parse([{**self._record(), "col_inconnue": "x"}])
        assert "col_inconnue" not in df.columns

    def test_multiple_records(self):
        assert len(parse(SAMPLE_RECORDS)) == 2


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

class TestRun:
    def test_raises_if_rid_not_set(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("RPLS_DIDO_RID", None)
            with pytest.raises(EnvironmentError, match="RPLS_DIDO_RID"):
                run()

    def test_full_refresh_skips_watermark(self):
        _, mocks = _run_mocked(watermark="2021", full_refresh=True)
        mocks["get_watermark"].assert_not_called()

    def test_incremental_reads_watermark(self):
        _, mocks = _run_mocked(watermark="2021")
        mocks["get_watermark"].assert_called_once_with(mocks["client"], SOURCE)

    def test_incremental_passes_filter(self):
        _, mocks = _run_mocked(watermark="2019")
        params = mocks["fetch_dido_pages"].call_args[0][1]
        assert "2019" in params.get("filters", "")

    def test_watermark_set_to_max_year(self):
        _, mocks = _run_mocked()
        mocks["set_watermark"].assert_called_once_with(mocks["client"], SOURCE, "2023")

    def test_dry_run_skips_insert_and_watermark(self):
        result, mocks = _run_mocked(dry_run=True)
        assert result == 0
        mocks["load_df"].assert_not_called()
        mocks["set_watermark"].assert_not_called()

    def test_creates_table_on_startup(self):
        _, mocks = _run_mocked()
        sql = mocks["client"].command.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert TABLE in sql

    def test_endpoint_contains_rid(self):
        _, mocks = _run_mocked(rid="rpls-rid-999")
        endpoint = mocks["fetch_dido_pages"].call_args[0][0]
        assert "rpls-rid-999" in endpoint
