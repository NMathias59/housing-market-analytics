"""Unit tests for include/ingestion/scripts/dvf.py"""

from __future__ import annotations

import gzip
import io
import os
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from include.ingestion.scripts.dvf import (
    SOURCE,
    TABLE,
    _years_to_load,
    build_file_url,
    iter_csv_chunks,
    parse,
    run,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CSV_HEADER = (
    "id_mutation,date_mutation,nature_mutation,valeur_fonciere,"
    "adresse_numero,adresse_nom_voie,code_postal,code_commune,nom_commune,"
    "code_departement,id_parcelle,type_local,surface_reelle_bati,"
    "nombre_pieces_principales,surface_terrain,latitude,longitude\n"
)
_CSV_ROW_2022 = (
    "2022-1234,2022-06-15,Vente,350000,"
    "12,RUE DE LA PAIX,75001,75056,Paris,"
    "75,75056000AB0042,Appartement,68.5,"
    "3,,,48.8698,2.3314\n"
)
_CSV_ROW_2023 = (
    "2023-5678,2023-03-20,Vente,280000,"
    "5,AVENUE JEAN JAURES,69007,69123,Lyon,"
    "69,69123000CD0015,Maison,95.0,"
    "4,220.0,45.7485,4.8467\n"
)


def _make_gz_csv(*rows: str) -> bytes:
    """Build a gzip-compressed CSV bytes object from header + rows."""
    content = _CSV_HEADER + "".join(rows)
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(content.encode("utf-8"))
    return buf.getvalue()


def _run_mocked(
    *,
    watermark: str | None = None,
    chunks: list[pd.DataFrame] | None = None,
    full_refresh: bool = False,
    dry_run: bool = False,
    year: int | None = None,
    target_year: int = 2023,
):
    if chunks is None:
        chunks = [
            pd.DataFrame({
                "id_mutation": ["2022-1"], "date_mutation": ["2022-06-15"],
                "nature_mutation": ["Vente"], "valeur_fonciere": ["350000"],
                "adresse_numero": ["12"], "adresse_nom_voie": ["RUE DE LA PAIX"],
                "code_postal": ["75001"], "code_commune": ["75056"],
                "nom_commune": ["Paris"], "code_departement": ["75"],
                "id_parcelle": ["75056AB42"], "type_local": ["Appartement"],
                "surface_reelle_bati": ["68.5"], "nombre_pieces_principales": ["3"],
                "surface_terrain": [""], "latitude": ["48.87"], "longitude": ["2.33"],
            })
        ]

    client = MagicMock()
    mocks: dict = {"client": client}

    with ExitStack() as stack:
        stack.enter_context(
            patch("include.ingestion.scripts.dvf.get_client", return_value=client)
        )
        stack.enter_context(patch("include.ingestion.scripts.dvf.ensure_watermark_table"))
        mocks["get_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.dvf.get_watermark", return_value=watermark)
        )
        mocks["iter_csv_chunks"] = stack.enter_context(
            patch("include.ingestion.scripts.dvf.iter_csv_chunks",
                  side_effect=lambda url, **kw: iter(chunks))
        )
        mocks["load_df"] = stack.enter_context(
            patch("include.ingestion.scripts.dvf.load_df",
                  side_effect=lambda client, df, table: len(df))
        )
        mocks["set_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.dvf.set_watermark")
        )
        stack.enter_context(
            patch("include.ingestion.scripts.dvf._target_year", return_value=target_year)
        )

        result = run(full_refresh=full_refresh, dry_run=dry_run, year=year)

    return result, mocks


# ---------------------------------------------------------------------------
# build_file_url
# ---------------------------------------------------------------------------

class TestBuildFileUrl:
    def test_contains_year(self):
        assert "2023" in build_file_url(2023)

    def test_ends_with_full_csv_gz(self):
        assert build_file_url(2022).endswith("/full.csv.gz")

    def test_uses_custom_base_url(self):
        with patch.dict(os.environ, {"DVF_BASE_URL": "http://custom-host/dvf"}):
            assert build_file_url(2022).startswith("http://custom-host/dvf")


# ---------------------------------------------------------------------------
# _years_to_load
# ---------------------------------------------------------------------------

class TestYearsToLoad:
    def test_full_load_starts_from_2021(self):
        years = _years_to_load(None, 2023)
        assert years[0] == 2021
        assert years[-1] == 2023

    def test_incremental_starts_after_watermark(self):
        years = _years_to_load(2021, 2023)
        assert years == [2022, 2023]

    def test_already_up_to_date_returns_empty(self):
        assert _years_to_load(2023, 2023) == []

    def test_single_year_remaining(self):
        assert _years_to_load(2022, 2023) == [2023]


# ---------------------------------------------------------------------------
# iter_csv_chunks
# ---------------------------------------------------------------------------

class TestIterCsvChunks:
    def test_yields_dataframe_chunks(self):
        gz_bytes = _make_gz_csv(_CSV_ROW_2022, _CSV_ROW_2023)
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.raise_for_status = MagicMock()
        mock_resp.raw = io.BytesIO(gz_bytes)

        with patch("requests.get", return_value=mock_resp):
            chunks = list(iter_csv_chunks("http://fake/dvf.csv.gz", chunk_size=100))

        assert len(chunks) == 1
        assert len(chunks[0]) == 2

    def test_chunk_size_limits_rows(self):
        gz_bytes = _make_gz_csv(_CSV_ROW_2022, _CSV_ROW_2023)
        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.raise_for_status = MagicMock()
        mock_resp.raw = io.BytesIO(gz_bytes)

        with patch("requests.get", return_value=mock_resp):
            chunks = list(iter_csv_chunks("http://fake/dvf.csv.gz", chunk_size=1))

        assert len(chunks) == 2
        assert all(len(c) == 1 for c in chunks)


# ---------------------------------------------------------------------------
# parse
# ---------------------------------------------------------------------------

class TestParse:
    def _chunk(self, **overrides) -> pd.DataFrame:
        data = {
            "id_mutation": ["2022-01"],
            "date_mutation": ["2022-06-15"],
            "nature_mutation": ["Vente"],
            "valeur_fonciere": ["350000"],
            "adresse_numero": ["12"],
            "adresse_nom_voie": ["RUE DE LA PAIX"],
            "code_postal": ["75001"],
            "code_commune": ["75056"],
            "nom_commune": ["Paris"],
            "code_departement": ["75"],
            "id_parcelle": ["75056AB42"],
            "type_local": ["Appartement"],
            "surface_reelle_bati": ["68.5"],
            "nombre_pieces_principales": ["3"],
            "surface_terrain": [""],
            "latitude": ["48.87"],
            "longitude": ["2.33"],
        }
        data.update(overrides)
        return pd.DataFrame(data)

    def test_derives_annee_from_date(self):
        df = parse(self._chunk(date_mutation=["2022-06-15"]))
        assert df["annee"].iloc[0] == 2022

    def test_parses_valeur_fonciere_as_float(self):
        df = parse(self._chunk(valeur_fonciere=["350000"]))
        assert df["valeur_fonciere"].iloc[0] == pytest.approx(350000.0)

    def test_handles_french_decimal_comma(self):
        df = parse(self._chunk(valeur_fonciere=["350 000,50"]))
        assert df["valeur_fonciere"].iloc[0] == pytest.approx(350000.50)

    def test_parses_pieces_as_int(self):
        df = parse(self._chunk(nombre_pieces_principales=["4"]))
        assert df["nombre_pieces_principales"].iloc[0] == 4

    def test_invalid_date_becomes_nat(self):
        df = parse(self._chunk(date_mutation=["not-a-date"]))
        assert pd.isna(df["date_mutation"].iloc[0])

    def test_missing_surface_becomes_nan(self):
        df = parse(self._chunk(surface_terrain=[""]))
        assert pd.isna(df["surface_terrain"].iloc[0])

    def test_unknown_columns_dropped(self):
        chunk = self._chunk()
        chunk["col_inconnue"] = "x"
        df = parse(chunk)
        assert "col_inconnue" not in df.columns

    def test_does_not_mutate_input(self):
        chunk = self._chunk()
        original_cols = list(chunk.columns)
        parse(chunk)
        assert list(chunk.columns) == original_cols


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

class TestRun:
    def test_creates_raw_table_on_startup(self):
        _, mocks = _run_mocked(watermark="2022")
        sql = mocks["client"].command.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert TABLE in sql

    def test_full_refresh_skips_watermark(self):
        _, mocks = _run_mocked(watermark="2021", full_refresh=True)
        mocks["get_watermark"].assert_not_called()

    def test_incremental_reads_watermark(self):
        _, mocks = _run_mocked(watermark="2022", target_year=2023)
        mocks["get_watermark"].assert_called_once_with(mocks["client"], SOURCE)

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
        wm_calls = [c[0][2] for c in mocks["set_watermark"].call_args_list]
        assert "2022" in wm_calls
        assert "2023" in wm_calls

    def test_specific_year_bypasses_watermark_logic(self):
        _, mocks = _run_mocked(watermark=None, year=2022)
        mocks["get_watermark"].assert_not_called()
        wm_set_years = [c[0][2] for c in mocks["set_watermark"].call_args_list]
        assert wm_set_years == []

    def test_returns_total_inserted_rows(self):
        result, _ = _run_mocked(watermark="2022", target_year=2023)
        assert result == 1

    def test_url_built_for_each_year(self):
        _, mocks = _run_mocked(watermark="2021", target_year=2023)
        urls = [c[0][0] for c in mocks["iter_csv_chunks"].call_args_list]
        assert any("2022" in u for u in urls)
        assert any("2023" in u for u in urls)
