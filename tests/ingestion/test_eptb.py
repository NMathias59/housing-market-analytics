"""Unit tests for include/ingestion/scripts/eptb.py"""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from include.ingestion.scripts.eptb import (
    TABLE, _RID_MAISONS, _RID_TERRAINS,
    _fetch_table, parse, run,
)

# ---------------------------------------------------------------------------
# Sample data matching DiDo API column names
# ---------------------------------------------------------------------------

SAMPLE_TERRAINS = [
    {"ANNEE": "2024", "ZONE_CODE": "11", "ZONE_LIBELLE": "Île-de-France",
     "NB_TERRAINS": 150, "PTM2_MOY": 350.0, "PTM2_MED": 320.0,
     "SURFT_MOY": 650.0, "PT_MOY": 227500.0},
]

SAMPLE_MAISONS = [
    {"ANNEE": "2024", "ZONE_CODE": "11", "ZONE_LIBELLE": "Île-de-France",
     "NB_MAISONS": 120, "PMM2_MOY": 1800.0, "PMM2_MED": 1750.0,
     "SURFM_MOY": 115.0, "PM_MOY": 207000.0},
]


def _df_terrains() -> pd.DataFrame:
    from include.ingestion.scripts.eptb import _RENAME_TERRAINS
    return pd.DataFrame(SAMPLE_TERRAINS).rename(columns=_RENAME_TERRAINS)


def _df_maisons() -> pd.DataFrame:
    from include.ingestion.scripts.eptb import _RENAME_MAISONS
    return pd.DataFrame(SAMPLE_MAISONS).rename(columns=_RENAME_MAISONS)


def _run_mocked(*, dry_run: bool = False, df_t=None, df_m=None):
    if df_t is None:
        df_t = _df_terrains()
    if df_m is None:
        df_m = _df_maisons()

    client = MagicMock()
    mocks: dict = {"client": client}

    with ExitStack() as stack:
        stack.enter_context(
            patch("include.ingestion.scripts.eptb.get_client", return_value=client)
        )
        stack.enter_context(
            patch("include.ingestion.scripts.eptb.ensure_watermark_table")
        )
        mocks["fetch_table"] = stack.enter_context(
            patch("include.ingestion.scripts.eptb._fetch_table",
                  side_effect=[df_t, df_m])
        )
        mocks["load_df"] = stack.enter_context(
            patch("include.ingestion.scripts.eptb.load_df",
                  side_effect=lambda c, df, t: len(df))
        )
        result = run(dry_run=dry_run)

    return result, mocks


# ---------------------------------------------------------------------------
# _fetch_table
# ---------------------------------------------------------------------------

class TestFetchTable:
    def test_returns_dataframe(self):
        pages = [SAMPLE_TERRAINS]
        from include.ingestion.scripts.eptb import _RENAME_TERRAINS
        with patch("include.ingestion.scripts.eptb.fetch_dido_pages",
                   return_value=iter(pages)):
            df = _fetch_table(_RID_TERRAINS, _RENAME_TERRAINS)
        assert isinstance(df, pd.DataFrame)
        assert "zone_code" in df.columns

    def test_empty_pages_returns_empty_df(self):
        from include.ingestion.scripts.eptb import _RENAME_TERRAINS
        with patch("include.ingestion.scripts.eptb.fetch_dido_pages",
                   return_value=iter([[]])):
            df = _fetch_table(_RID_TERRAINS, _RENAME_TERRAINS)
        assert df.empty

    def test_renames_columns(self):
        from include.ingestion.scripts.eptb import _RENAME_TERRAINS
        with patch("include.ingestion.scripts.eptb.fetch_dido_pages",
                   return_value=iter([SAMPLE_TERRAINS])):
            df = _fetch_table(_RID_TERRAINS, _RENAME_TERRAINS)
        assert "nb_terrains" in df.columns
        assert "NB_TERRAINS" not in df.columns


# ---------------------------------------------------------------------------
# parse
# ---------------------------------------------------------------------------

class TestParse:
    def test_returns_dataframe(self):
        assert isinstance(parse(_df_terrains(), _df_maisons()), pd.DataFrame)

    def test_merges_on_annee_and_zone_code(self):
        df = parse(_df_terrains(), _df_maisons())
        assert "nb_terrains" in df.columns
        assert "nb_maisons" in df.columns

    def test_annee_cast_to_int(self):
        df = parse(_df_terrains(), _df_maisons())
        assert df["annee"].iloc[0] == 2024

    def test_float_cols_cast(self):
        df = parse(_df_terrains(), _df_maisons())
        assert df["prix_terrain_m2_moy"].iloc[0] == pytest.approx(350.0)
        assert df["prix_maison_m2_moy"].iloc[0] == pytest.approx(1800.0)

    def test_int_cols_cast(self):
        df = parse(_df_terrains(), _df_maisons())
        assert df["nb_terrains"].iloc[0] == 150
        assert df["nb_maisons"].iloc[0] == 120

    def test_zone_code_is_string(self):
        df = parse(_df_terrains(), _df_maisons())
        assert df["zone_code"].iloc[0] == "11"

    def test_both_empty_returns_empty(self):
        assert parse(pd.DataFrame(), pd.DataFrame()).empty

    def test_outer_join_fills_missing_side(self):
        other_maisons = _df_maisons().copy()
        other_maisons["zone_code"] = "76"
        df = parse(_df_terrains(), other_maisons)
        assert len(df) == 2
        assert df[df["zone_code"] == "11"]["nb_maisons"].isna().all()

    def test_zone_libelle_is_string(self):
        df = parse(_df_terrains(), _df_maisons())
        assert df["zone_libelle"].iloc[0] == "Île-de-France"


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

class TestRun:
    def test_creates_table_on_startup(self):
        _, mocks = _run_mocked()
        sql = mocks["client"].command.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert TABLE in sql

    def test_fetches_both_rids(self):
        _, mocks = _run_mocked()
        called_rids = [c[0][0] for c in mocks["fetch_table"].call_args_list]
        assert _RID_TERRAINS in called_rids
        assert _RID_MAISONS in called_rids

    def test_dry_run_skips_insert(self):
        result, mocks = _run_mocked(dry_run=True)
        assert result == 0
        mocks["load_df"].assert_not_called()

    def test_inserts_merged_data(self):
        result, mocks = _run_mocked()
        assert result == 1
        mocks["load_df"].assert_called_once()

    def test_no_watermark_env_var_needed(self):
        """EPTB has no env var requirement — RIDs are hardcoded."""
        result, _ = _run_mocked()
        assert result >= 0

    def test_empty_terrains_returns_zero(self):
        result, _ = _run_mocked(df_t=pd.DataFrame(), df_m=pd.DataFrame())
        assert result == 0
