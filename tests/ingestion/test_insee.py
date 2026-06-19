"""Unit tests for include/ingestion/scripts/insee.py"""

from __future__ import annotations

import os
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from include.ingestion.scripts.insee import (
    SOURCE, TABLE,
    _years_to_load, build_file_url, parse, run,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_chunk(**overrides) -> pd.DataFrame:
    data = {
        "code_commune": ["01001"],
        "libelle_commune": ["L'Abergement-Clémenciat"],
        "annee": ["2021"],
        "revenu_disponible_median": ["21350,5"],
        "taux_pauvrete": ["10,2"],
        "population_municipale": ["820"],
        "superficie": ["15,32"],
        "densite_population": ["53,5"],
        "part_proprietaires": ["68,4"],
        "solde_migratoire_net": ["12"],
        "part_logements_vacants": ["6,8"],
    }
    data.update(overrides)
    return pd.DataFrame(data)


def _run_mocked(
    *,
    watermark: str | None = None,
    chunks: list | None = None,
    full_refresh: bool = False,
    dry_run: bool = False,
    year: int | None = None,
    target_year: int = 2021,
):
    if chunks is None:
        chunks = [_make_chunk()]

    client = MagicMock()
    mocks: dict = {"client": client}

    with ExitStack() as stack:
        stack.enter_context(
            patch.dict(os.environ, {"INSEE_BASE_URL": "http://fake-insee"})
        )
        stack.enter_context(
            patch("include.ingestion.scripts.insee.get_client", return_value=client)
        )
        stack.enter_context(patch("include.ingestion.scripts.insee.ensure_watermark_table"))
        mocks["get_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.insee.get_watermark", return_value=watermark)
        )
        mocks["iter_csv_chunks"] = stack.enter_context(
            patch("include.ingestion.scripts.insee.iter_csv_chunks",
                  side_effect=lambda url, size: iter(chunks))
        )
        mocks["load_df"] = stack.enter_context(
            patch("include.ingestion.scripts.insee.load_df",
                  side_effect=lambda c, df, t: len(df))
        )
        mocks["set_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.insee.set_watermark")
        )
        stack.enter_context(
            patch("include.ingestion.scripts.insee._target_year", return_value=target_year)
        )
        result = run(full_refresh=full_refresh, dry_run=dry_run, year=year)

    return result, mocks


# ---------------------------------------------------------------------------
# build_file_url
# ---------------------------------------------------------------------------

class TestBuildFileUrl:
    def test_contains_year(self):
        with patch.dict(os.environ, {"INSEE_BASE_URL": "http://insee/data"}):
            assert "2021" in build_file_url(2021)

    def test_ends_with_csv(self):
        with patch.dict(os.environ, {"INSEE_BASE_URL": "http://insee/data"}):
            url = build_file_url(2020)
            assert url.endswith(".csv")
            assert not url.endswith(".csv.gz")

    def test_raises_if_base_url_not_set(self):
        env = {k: v for k, v in os.environ.items() if k != "INSEE_BASE_URL"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(EnvironmentError, match="INSEE_BASE_URL"):
                build_file_url(2021)


# ---------------------------------------------------------------------------
# _years_to_load
# ---------------------------------------------------------------------------

class TestYearsToLoad:
    def test_full_load_starts_from_2017(self):
        years = _years_to_load(None, 2021)
        assert years[0] == 2017
        assert years[-1] == 2021

    def test_incremental_starts_after_watermark(self):
        assert _years_to_load(2020, 2021) == [2021]

    def test_up_to_date_returns_empty(self):
        assert _years_to_load(2021, 2021) == []


# ---------------------------------------------------------------------------
# parse
# ---------------------------------------------------------------------------

class TestParse:
    def test_returns_dataframe(self):
        assert isinstance(parse(_make_chunk()), pd.DataFrame)

    def test_parses_annee_as_int(self):
        df = parse(_make_chunk(annee=["2021"]))
        assert df["annee"].iloc[0] == 2021

    def test_converts_french_decimal_comma_float(self):
        df = parse(_make_chunk(revenu_disponible_median=["21350,5"]))
        assert df["revenu_disponible_median"].iloc[0] == pytest.approx(21350.5)

    def test_invalid_float_becomes_nan(self):
        df = parse(_make_chunk(taux_pauvrete=["ND"]))
        assert pd.isna(df["taux_pauvrete"].iloc[0])

    def test_parses_int_col(self):
        df = parse(_make_chunk(population_municipale=["820"]))
        assert df["population_municipale"].iloc[0] == 820

    def test_null_string_becomes_empty(self):
        df = parse(_make_chunk(libelle_commune=[None]))
        assert df["libelle_commune"].iloc[0] == ""

    def test_unknown_columns_dropped(self):
        chunk = _make_chunk()
        chunk["champ_inattendu"] = "x"
        assert "champ_inattendu" not in parse(chunk).columns

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
        _, mocks = _run_mocked(watermark="2020")
        sql = mocks["client"].command.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert TABLE in sql

    def test_full_refresh_skips_watermark_read(self):
        _, mocks = _run_mocked(watermark="2020", full_refresh=True)
        mocks["get_watermark"].assert_not_called()

    def test_up_to_date_inserts_nothing(self):
        result, mocks = _run_mocked(watermark="2021", target_year=2021)
        assert result == 0
        mocks["load_df"].assert_not_called()

    def test_dry_run_skips_insert_and_watermark(self):
        result, mocks = _run_mocked(watermark="2020", target_year=2021, dry_run=True)
        assert result == 0
        mocks["load_df"].assert_not_called()
        mocks["set_watermark"].assert_not_called()

    def test_watermark_set_per_year(self):
        _, mocks = _run_mocked(watermark="2019", target_year=2021)
        wm_years = [c[0][2] for c in mocks["set_watermark"].call_args_list]
        assert "2020" in wm_years
        assert "2021" in wm_years

    def test_specific_year_skips_watermark_set(self):
        _, mocks = _run_mocked(year=2020)
        mocks["set_watermark"].assert_not_called()

    def test_returns_total_rows(self):
        result, _ = _run_mocked(watermark="2020", target_year=2021)
        assert result == 1
