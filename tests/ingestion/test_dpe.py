"""Unit tests for include/ingestion/scripts/dpe.py"""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from include.ingestion.scripts.dpe import (
    SOURCE, TABLE,
    _dept_from_commune, _since_date,
    parse, run,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _records(**overrides) -> list[dict]:
    base = {
        "numero_dpe":                       "2023-75-001",
        "date_etablissement_dpe":           "2023-04-12",
        "code_insee_ban":                   "75056",
        "etiquette_dpe":                    "C",
        "conso_5_usages_par_m2_ep":         145.2,
        "conso_5_usages_par_m2_ef":         98.5,
        "emission_ges_5_usages_par_m2":     28.0,
        "type_energie_principale_chauffage":"gaz naturel",
        "type_installation_chauffage":       "individuel",
        "surface_habitable_immeuble":        72.5,
        "periode_construction":              "1975-1977",
        "type_batiment":                     "appartement",
    }
    base.update(overrides)
    return [base]


def _run_mocked(
    *,
    watermark: str | None = None,
    pages: list[list[dict]] | None = None,
    full_refresh: bool = False,
    dry_run: bool = False,
    since: str | None = None,
):
    if pages is None:
        pages = [_records()]

    client = MagicMock()
    mocks: dict = {"client": client}

    with ExitStack() as stack:
        stack.enter_context(
            patch("include.ingestion.scripts.dpe.get_client", return_value=client)
        )
        stack.enter_context(
            patch("include.ingestion.scripts.dpe.ensure_watermark_table")
        )
        mocks["get_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.dpe.get_watermark",
                  return_value=watermark)
        )
        mocks["fetch_ademe_pages"] = stack.enter_context(
            patch("include.ingestion.scripts.dpe.fetch_ademe_pages",
                  return_value=iter(pages))
        )
        mocks["load_df"] = stack.enter_context(
            patch("include.ingestion.scripts.dpe.load_df",
                  side_effect=lambda c, df, t: len(df))
        )
        mocks["set_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.dpe.set_watermark")
        )
        result = run(full_refresh=full_refresh, dry_run=dry_run, since=since)

    return result, mocks


# ---------------------------------------------------------------------------
# _dept_from_commune
# ---------------------------------------------------------------------------

class TestDeptFromCommune:
    def test_mainland_2digits(self):
        assert _dept_from_commune("75056") == "75"

    def test_dom_3digits(self):
        assert _dept_from_commune("97100") == "971"

    def test_corsica(self):
        assert _dept_from_commune("2A004") == "2A"


# ---------------------------------------------------------------------------
# _since_date
# ---------------------------------------------------------------------------

class TestSinceDate:
    def test_none_returns_default(self):
        from include.ingestion.scripts.dpe import _SINCE_DATE_DEFAULT
        assert _since_date(None) == _SINCE_DATE_DEFAULT

    def test_returns_next_day(self):
        assert _since_date("2023-06-15") == "2023-06-16"

    def test_month_boundary(self):
        assert _since_date("2023-01-31") == "2023-02-01"


# ---------------------------------------------------------------------------
# parse
# ---------------------------------------------------------------------------

class TestParse:
    def test_returns_dataframe(self):
        assert isinstance(parse(_records()), pd.DataFrame)

    def test_empty_input(self):
        assert parse([]).empty

    def test_renames_code_insee_ban(self):
        df = parse(_records(code_insee_ban="69001"))
        assert df["code_commune"].iloc[0] == "69001"

    def test_derives_code_departement(self):
        df = parse(_records(code_insee_ban="13055"))
        assert df["code_departement"].iloc[0] == "13"

    def test_derives_dept_dom(self):
        df = parse(_records(code_insee_ban="97200"))
        assert df["code_departement"].iloc[0] == "972"

    def test_derives_annee_from_date(self):
        df = parse(_records(date_etablissement_dpe="2022-11-30"))
        assert df["annee"].iloc[0] == 2022

    def test_renames_conso_energie_primaire(self):
        df = parse(_records(conso_5_usages_par_m2_ep=180.5))
        assert df["consommation_energie_primaire"].iloc[0] == pytest.approx(180.5)

    def test_renames_conso_energie_finale(self):
        df = parse(_records(conso_5_usages_par_m2_ef=120.0))
        assert df["consommation_energie_finale"].iloc[0] == pytest.approx(120.0)

    def test_renames_emission_ges(self):
        df = parse(_records(emission_ges_5_usages_par_m2=35.0))
        assert df["emission_ges"].iloc[0] == pytest.approx(35.0)

    def test_periode_construction_is_string(self):
        df = parse(_records(periode_construction="avant 1948"))
        assert df["periode_construction"].iloc[0] == "avant 1948"

    def test_null_float_becomes_nan(self):
        df = parse(_records(conso_5_usages_par_m2_ep=None))
        assert pd.isna(df["consommation_energie_primaire"].iloc[0])

    def test_null_commune_becomes_empty_string(self):
        df = parse(_records(code_insee_ban=None))
        assert df["code_commune"].iloc[0] == ""

    def test_unknown_columns_dropped(self):
        recs = _records()
        recs[0]["champ_inconnu"] = "x"
        assert "champ_inconnu" not in parse(recs).columns

    def test_does_not_mutate_input(self):
        recs = _records()
        original_keys = list(recs[0].keys())
        parse(recs)
        assert list(recs[0].keys()) == original_keys


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

class TestRun:
    def test_creates_table_on_startup(self):
        _, mocks = _run_mocked()
        sql = mocks["client"].command.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert TABLE in sql

    def test_full_refresh_skips_watermark_read(self):
        _, mocks = _run_mocked(full_refresh=True)
        mocks["get_watermark"].assert_not_called()

    def test_full_refresh_has_no_qs_filter(self):
        _, mocks = _run_mocked(full_refresh=True)
        params = mocks["fetch_ademe_pages"].call_args[0][1]
        assert "qs" not in params

    def test_incremental_reads_watermark(self):
        _, mocks = _run_mocked(watermark="2023-06-01")
        mocks["get_watermark"].assert_called_once_with(mocks["client"], SOURCE)

    def test_incremental_passes_qs_filter(self):
        _, mocks = _run_mocked(watermark="2023-06-01")
        params = mocks["fetch_ademe_pages"].call_args[0][1]
        assert "qs" in params
        assert "date_etablissement_dpe" in params["qs"]
        assert "2023-06-02" in params["qs"]

    def test_watermark_none_uses_default_date(self):
        from include.ingestion.scripts.dpe import _SINCE_DATE_DEFAULT
        _, mocks = _run_mocked(watermark=None)
        params = mocks["fetch_ademe_pages"].call_args[0][1]
        assert _SINCE_DATE_DEFAULT in params["qs"]

    def test_since_flag_overrides_watermark(self):
        _, mocks = _run_mocked(since="2022-01-01")
        params = mocks["fetch_ademe_pages"].call_args[0][1]
        assert "2022-01-02" in params["qs"]

    def test_sort_ascending_by_date(self):
        _, mocks = _run_mocked()
        params = mocks["fetch_ademe_pages"].call_args[0][1]
        assert params.get("sort") == "date_etablissement_dpe"

    def test_watermark_updated_after_each_page(self):
        _, mocks = _run_mocked()
        assert mocks["set_watermark"].call_count == 1
        wm_val = mocks["set_watermark"].call_args[0][2]
        assert wm_val == "2023-04-12"

    def test_dry_run_skips_insert_and_watermark(self):
        result, mocks = _run_mocked(dry_run=True)
        assert result == 0
        mocks["load_df"].assert_not_called()
        mocks["set_watermark"].assert_not_called()

    def test_returns_row_count(self):
        result, _ = _run_mocked()
        assert result == 1

    def test_empty_page_no_insert(self):
        result, mocks = _run_mocked(pages=[[]])
        assert result == 0
        mocks["load_df"].assert_not_called()
