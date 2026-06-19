"""Unit tests for include/ingestion/scripts/dpe.py"""

from __future__ import annotations

import os
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from include.ingestion.scripts.dpe import (
    SOURCE, TABLE,
    _years_to_load, build_file_url, parse, run,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_chunk(**overrides) -> pd.DataFrame:
    data = {
        "numero_dpe": ["2023-75-001"],
        "date_etablissement_dpe": ["2023-04-12"],
        "code_commune": ["75056"],
        "code_departement": ["75"],
        "etiquette_dpe": ["C"],
        "consommation_energie_primaire": ["145.2"],
        "consommation_energie_finale": ["98.5"],
        "emission_ges": ["28.0"],
        "type_energie_principale_chauffage": ["gaz naturel"],
        "type_installation_chauffage": ["individuel"],
        "surface_habitable_logement": ["72.5"],
        "annee_construction": ["1985"],
        "type_batiment": ["appartement"],
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
    target_year: int = 2023,
):
    if chunks is None:
        chunks = [_make_chunk()]

    client = MagicMock()
    mocks: dict = {"client": client}

    with ExitStack() as stack:
        stack.enter_context(
            patch.dict(os.environ, {"DPE_BASE_URL": "http://fake-ademe"})
        )
        stack.enter_context(
            patch("include.ingestion.scripts.dpe.get_client", return_value=client)
        )
        stack.enter_context(patch("include.ingestion.scripts.dpe.ensure_watermark_table"))
        mocks["get_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.dpe.get_watermark", return_value=watermark)
        )
        mocks["iter_csv_chunks"] = stack.enter_context(
            patch("include.ingestion.scripts.dpe.iter_csv_chunks",
                  side_effect=lambda url, size: iter(chunks))
        )
        mocks["load_df"] = stack.enter_context(
            patch("include.ingestion.scripts.dpe.load_df",
                  side_effect=lambda c, df, t: len(df))
        )
        mocks["set_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.dpe.set_watermark")
        )
        stack.enter_context(
            patch("include.ingestion.scripts.dpe._target_year", return_value=target_year)
        )
        result = run(full_refresh=full_refresh, dry_run=dry_run, year=year)

    return result, mocks


# ---------------------------------------------------------------------------
# build_file_url
# ---------------------------------------------------------------------------

class TestBuildFileUrl:
    def test_contains_year(self):
        with patch.dict(os.environ, {"DPE_BASE_URL": "http://ademe/dpe"}):
            assert "2023" in build_file_url(2023)

    def test_ends_with_csv_gz(self):
        with patch.dict(os.environ, {"DPE_BASE_URL": "http://ademe/dpe"}):
            assert build_file_url(2022).endswith(".csv.gz")

    def test_raises_if_base_url_not_set(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("DPE_BASE_URL", None)
            with pytest.raises(EnvironmentError, match="DPE_BASE_URL"):
                build_file_url(2023)


# ---------------------------------------------------------------------------
# _years_to_load
# ---------------------------------------------------------------------------

class TestYearsToLoad:
    def test_full_load_starts_from_2021(self):
        years = _years_to_load(None, 2023)
        assert years[0] == 2021
        assert years[-1] == 2023

    def test_incremental_starts_after_watermark(self):
        assert _years_to_load(2022, 2023) == [2023]

    def test_up_to_date_returns_empty(self):
        assert _years_to_load(2023, 2023) == []


# ---------------------------------------------------------------------------
# parse
# ---------------------------------------------------------------------------

class TestParse:
    def test_returns_dataframe(self):
        assert isinstance(parse(_make_chunk()), pd.DataFrame)

    def test_derives_annee_from_date(self):
        df = parse(_make_chunk(date_etablissement_dpe=["2023-06-01"]))
        assert df["annee"].iloc[0] == 2023

    def test_parses_consommation_as_float(self):
        df = parse(_make_chunk(consommation_energie_primaire=["145.2"]))
        assert df["consommation_energie_primaire"].iloc[0] == pytest.approx(145.2)

    def test_handles_decimal_comma(self):
        df = parse(_make_chunk(consommation_energie_primaire=["145,2"]))
        assert df["consommation_energie_primaire"].iloc[0] == pytest.approx(145.2)

    def test_invalid_numeric_becomes_nan(self):
        df = parse(_make_chunk(emission_ges=["N/D"]))
        assert pd.isna(df["emission_ges"].iloc[0])

    def test_parses_annee_construction_as_int(self):
        df = parse(_make_chunk(annee_construction=["1972"]))
        assert df["annee_construction"].iloc[0] == 1972

    def test_null_string_becomes_empty(self):
        df = parse(_make_chunk(etiquette_dpe=[None]))
        assert df["etiquette_dpe"].iloc[0] == ""

    def test_unknown_columns_dropped(self):
        chunk = _make_chunk()
        chunk["champ_inconnu"] = "x"
        assert "champ_inconnu" not in parse(chunk).columns

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
        _, mocks = _run_mocked(watermark="2022")
        sql = mocks["client"].command.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert TABLE in sql

    def test_full_refresh_skips_watermark(self):
        _, mocks = _run_mocked(watermark="2021", full_refresh=True)
        mocks["get_watermark"].assert_not_called()

    def test_up_to_date_inserts_nothing(self):
        result, mocks = _run_mocked(watermark="2023", target_year=2023)
        assert result == 0
        mocks["load_df"].assert_not_called()

    def test_dry_run_skips_insert_and_watermark(self):
        result, mocks = _run_mocked(watermark="2022", target_year=2023, dry_run=True)
        assert result == 0
        mocks["load_df"].assert_not_called()
        mocks["set_watermark"].assert_not_called()

    def test_watermark_updated_per_year(self):
        _, mocks = _run_mocked(watermark="2021", target_year=2023)
        wm_years = [c[0][2] for c in mocks["set_watermark"].call_args_list]
        assert "2022" in wm_years
        assert "2023" in wm_years

    def test_specific_year_skips_watermark_set(self):
        _, mocks = _run_mocked(year=2022)
        mocks["set_watermark"].assert_not_called()

    def test_returns_total_rows(self):
        result, _ = _run_mocked(watermark="2022", target_year=2023)
        assert result == 1
