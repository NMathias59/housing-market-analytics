"""Unit tests for include/ingestion/scripts/eptb.py"""

from __future__ import annotations

import os
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from include.ingestion.scripts.eptb import (
    SOURCE,
    TABLE,
    build_params,
    parse,
    run,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_RECORDS = [
    {
        "annee": "2022",
        "code_commune": "75056",
        "code_departement": "75",
        "code_region": "11",
        "prix_terrain_m2": "310.5",
        "surface_terrain": "280.0",
        "prix_construction": "195000.0",
        "surface_habitable": "115.0",
        "type_maitre_oeuvre": "particulier",
        "type_chauffage": "gaz",
        "csp_acheteur": "cadre",
    },
    {
        "annee": "2023",
        "code_commune": "69123",
        "code_departement": "69",
        "code_region": "84",
        "prix_terrain_m2": "220.0",
        "surface_terrain": "400.0",
        "prix_construction": "180000.0",
        "surface_habitable": "130.0",
        "type_maitre_oeuvre": "constructeur",
        "type_chauffage": "pompe_chaleur",
        "csp_acheteur": "employe",
    },
]


def _run_mocked(
    *,
    watermark: str | None = None,
    records: list | None = None,
    full_refresh: bool = False,
    dry_run: bool = False,
    rid: str = "fake-rid-123",
):
    """Call run() with all external dependencies mocked. Returns (result, mocks)."""
    if records is None:
        records = SAMPLE_RECORDS

    client = MagicMock()
    mocks: dict = {"client": client}

    with ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, {"EPTB_DIDO_RID": rid}))
        stack.enter_context(
            patch("include.ingestion.scripts.eptb.get_client", return_value=client)
        )
        stack.enter_context(
            patch("include.ingestion.scripts.eptb.ensure_watermark_table")
        )
        mocks["get_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.eptb.get_watermark", return_value=watermark)
        )
        mocks["fetch_dido_pages"] = stack.enter_context(
            patch(
                "include.ingestion.scripts.eptb.fetch_dido_pages",
                return_value=iter([records]),
            )
        )
        mocks["load_df"] = stack.enter_context(
            patch("include.ingestion.scripts.eptb.load_df", return_value=len(records))
        )
        mocks["set_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.eptb.set_watermark")
        )

        result = run(full_refresh=full_refresh, dry_run=dry_run)

    return result, mocks


# ---------------------------------------------------------------------------
# build_params
# ---------------------------------------------------------------------------

class TestBuildParams:
    def test_no_watermark_returns_empty_dict(self):
        assert build_params(None) == {}

    def test_with_watermark_adds_gt_filter(self):
        params = build_params(2022)
        assert params == {"filters": "annee:gt:2022"}

    def test_filter_contains_year_value(self):
        params = build_params(2019)
        assert "2019" in params["filters"]

    def test_filter_uses_gt_operator(self):
        params = build_params(2022)
        assert ":gt:" in params["filters"]


# ---------------------------------------------------------------------------
# parse
# ---------------------------------------------------------------------------

class TestParse:
    def _record(self, **overrides) -> dict:
        base = {
            "annee": "2022",
            "code_commune": "75056",
            "code_departement": "75",
            "code_region": "11",
            "prix_terrain_m2": "250.5",
            "surface_terrain": "300.0",
            "prix_construction": "180000.0",
            "surface_habitable": "120.0",
            "type_maitre_oeuvre": "particulier",
            "type_chauffage": "gaz",
            "csp_acheteur": "cadre",
        }
        base.update(overrides)
        return base

    def test_returns_dataframe(self):
        assert isinstance(parse([self._record()]), pd.DataFrame)

    def test_empty_input_returns_empty_df(self):
        assert parse([]).empty

    def test_parses_annee_as_integer(self):
        df = parse([self._record(annee="2022")])
        assert df["annee"].iloc[0] == 2022

    def test_parses_float_columns(self):
        df = parse([self._record(prix_terrain_m2="250.5")])
        assert df["prix_terrain_m2"].iloc[0] == pytest.approx(250.5)

    def test_invalid_numeric_becomes_nan(self):
        df = parse([self._record(prix_terrain_m2="N/D")])
        assert pd.isna(df["prix_terrain_m2"].iloc[0])

    def test_null_string_col_becomes_empty_string(self):
        df = parse([self._record(code_region=None)])
        assert df["code_region"].iloc[0] == ""

    def test_unknown_columns_are_dropped(self):
        records = [{**self._record(), "colonne_inconnue": "valeur"}]
        df = parse(records)
        assert "colonne_inconnue" not in df.columns

    def test_partial_columns_no_error(self):
        df = parse([{"annee": "2021", "code_commune": "33063"}])
        assert list(df.columns) == ["annee", "code_commune"]

    def test_multiple_records_correct_row_count(self):
        df = parse([self._record(annee=str(y)) for y in [2020, 2021, 2022]])
        assert len(df) == 3

    def test_annee_values_preserved(self):
        df = parse([self._record(annee=str(y)) for y in [2020, 2021, 2022]])
        assert list(df["annee"]) == [2020, 2021, 2022]

    def test_does_not_mutate_input_records(self):
        record = self._record()
        original_keys = set(record.keys())
        parse([record])
        assert set(record.keys()) == original_keys


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

class TestRun:
    def test_raises_if_dido_rid_not_set(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("EPTB_DIDO_RID", None)
            with pytest.raises(EnvironmentError, match="EPTB_DIDO_RID"):
                run()

    def test_full_refresh_skips_get_watermark(self):
        _, mocks = _run_mocked(watermark="2021", full_refresh=True)
        mocks["get_watermark"].assert_not_called()

    def test_incremental_reads_watermark(self):
        _, mocks = _run_mocked(watermark="2021")
        mocks["get_watermark"].assert_called_once_with(mocks["client"], SOURCE)

    def test_incremental_passes_year_filter_to_api(self):
        _, mocks = _run_mocked(watermark="2021")
        call_params = mocks["fetch_dido_pages"].call_args[0][1]
        assert "2021" in call_params.get("filters", "")

    def test_full_refresh_sends_no_filter(self):
        _, mocks = _run_mocked(watermark="2021", full_refresh=True)
        call_params = mocks["fetch_dido_pages"].call_args[0][1]
        assert call_params == {}

    def test_creates_raw_table_on_startup(self):
        _, mocks = _run_mocked()
        mocks["client"].command.assert_called_once()
        sql = mocks["client"].command.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert TABLE in sql

    def test_watermark_set_to_max_year_after_insert(self):
        _, mocks = _run_mocked(watermark=None)
        mocks["set_watermark"].assert_called_once_with(mocks["client"], SOURCE, "2023")

    def test_watermark_not_updated_on_dry_run(self):
        _, mocks = _run_mocked(dry_run=True)
        mocks["set_watermark"].assert_not_called()

    def test_dry_run_skips_insert(self):
        _, mocks = _run_mocked(dry_run=True)
        mocks["load_df"].assert_not_called()

    def test_dry_run_returns_zero(self):
        result, _ = _run_mocked(dry_run=True)
        assert result == 0

    def test_returns_total_inserted_rows(self):
        result, _ = _run_mocked()
        assert result == len(SAMPLE_RECORDS)

    def test_watermark_not_updated_when_no_data(self):
        _, mocks = _run_mocked(records=[])
        mocks["set_watermark"].assert_not_called()

    def test_endpoint_built_from_rid(self):
        _, mocks = _run_mocked(rid="my-custom-rid")
        endpoint_called = mocks["fetch_dido_pages"].call_args[0][0]
        assert "my-custom-rid" in endpoint_called
        assert endpoint_called.endswith("/rows")
