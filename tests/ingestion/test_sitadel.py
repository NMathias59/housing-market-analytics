"""Unit tests for include/ingestion/scripts/sitadel.py"""

from __future__ import annotations

import os
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from include.ingestion.scripts.sitadel import SOURCE, TABLE, build_params, parse, run

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_RECORDS = [
    {
        "annee": "2022",
        "mois": "6",
        "code_commune": "75056",
        "code_departement": "75",
        "type_logement": "collectif",
        "nb_logements_autorises": "320",
        "nb_logements_commences": "280",
        "surface_autorisee": "24500.0",
    },
    {
        "annee": "2023",
        "mois": "3",
        "code_commune": "69123",
        "code_departement": "69",
        "type_logement": "individuel",
        "nb_logements_autorises": "85",
        "nb_logements_commences": "70",
        "surface_autorisee": "9800.0",
    },
]


def _run_mocked(
    *,
    watermark: str | None = None,
    records: list | None = None,
    full_refresh: bool = False,
    dry_run: bool = False,
    rid: str = "fake-sitadel-rid",
):
    if records is None:
        records = SAMPLE_RECORDS
    client = MagicMock()
    mocks: dict = {"client": client}

    with ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, {"SITADEL_DIDO_RID": rid}))
        stack.enter_context(
            patch("include.ingestion.scripts.sitadel.get_client", return_value=client)
        )
        stack.enter_context(patch("include.ingestion.scripts.sitadel.ensure_watermark_table"))
        mocks["get_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.sitadel.get_watermark", return_value=watermark)
        )
        mocks["fetch_dido_pages"] = stack.enter_context(
            patch("include.ingestion.scripts.sitadel.fetch_dido_pages",
                  return_value=iter([records]))
        )
        mocks["load_df"] = stack.enter_context(
            patch("include.ingestion.scripts.sitadel.load_df", return_value=len(records))
        )
        mocks["set_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.sitadel.set_watermark")
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
        assert build_params(2022) == {"filters": "annee:gt:2022"}


# ---------------------------------------------------------------------------
# parse
# ---------------------------------------------------------------------------

class TestParse:
    def _record(self, **kw):
        base = {
            "annee": "2022", "mois": "4",
            "code_commune": "13055", "code_departement": "13",
            "type_logement": "collectif",
            "nb_logements_autorises": "150",
            "nb_logements_commences": "120",
            "surface_autorisee": "12000.0",
        }
        base.update(kw)
        return base

    def test_returns_dataframe(self):
        assert isinstance(parse([self._record()]), pd.DataFrame)

    def test_empty_input_returns_empty_df(self):
        assert parse([]).empty

    def test_parses_annee(self):
        assert parse([self._record(annee="2021")])["annee"].iloc[0] == 2021

    def test_parses_mois_as_int(self):
        assert parse([self._record(mois="7")])["mois"].iloc[0] == 7

    def test_parses_surface_as_float(self):
        df = parse([self._record(surface_autorisee="9876.5")])
        assert df["surface_autorisee"].iloc[0] == pytest.approx(9876.5)

    def test_invalid_numeric_becomes_nan(self):
        df = parse([self._record(nb_logements_autorises="NC")])
        assert pd.isna(df["nb_logements_autorises"].iloc[0])

    def test_null_commune_becomes_empty(self):
        df = parse([self._record(code_commune=None)])
        assert df["code_commune"].iloc[0] == ""

    def test_unknown_columns_dropped(self):
        df = parse([{**self._record(), "champ_inconnu": "val"}])
        assert "champ_inconnu" not in df.columns

    def test_multiple_records(self):
        assert len(parse(SAMPLE_RECORDS)) == 2


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

class TestRun:
    def test_raises_if_rid_not_set(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("SITADEL_DIDO_RID", None)
            with pytest.raises(EnvironmentError, match="SITADEL_DIDO_RID"):
                run()

    def test_full_refresh_skips_watermark(self):
        _, mocks = _run_mocked(watermark="2021", full_refresh=True)
        mocks["get_watermark"].assert_not_called()

    def test_incremental_reads_watermark(self):
        _, mocks = _run_mocked(watermark="2021")
        mocks["get_watermark"].assert_called_once_with(mocks["client"], SOURCE)

    def test_incremental_passes_filter(self):
        _, mocks = _run_mocked(watermark="2020")
        params = mocks["fetch_dido_pages"].call_args[0][1]
        assert "2020" in params.get("filters", "")

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
        _, mocks = _run_mocked(rid="sitadel-rid-abc")
        endpoint = mocks["fetch_dido_pages"].call_args[0][0]
        assert "sitadel-rid-abc" in endpoint
