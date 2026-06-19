"""Unit tests for include/ingestion/scripts/sitadel.py"""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from include.ingestion.scripts.sitadel import (
    SOURCE, TABLE,
    _dept_from_commune, _parse_watermark,
    parse, run,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _records(**overrides) -> list[dict]:
    base = {
        "ANNEE": "2024", "MOIS": "03",
        "CODE_INSEE": "75056", "TYPE_LGT": "Logement individuel",
        "LOG_AUT": 5, "LOG_COM": 3, "SDP_AUT": 450, "SDP_COM": 270,
    }
    base.update(overrides)
    return [base]


def _run_mocked(
    *,
    watermark: str | None = None,
    pages: list[list[dict]] | None = None,
    full_refresh: bool = False,
    dry_run: bool = False,
):
    if pages is None:
        pages = [_records()]

    client = MagicMock()
    mocks: dict = {"client": client}

    with ExitStack() as stack:
        stack.enter_context(
            patch("include.ingestion.scripts.sitadel.get_client", return_value=client)
        )
        stack.enter_context(
            patch("include.ingestion.scripts.sitadel.ensure_watermark_table")
        )
        mocks["get_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.sitadel.get_watermark",
                  return_value=watermark)
        )
        mocks["fetch_dido_pages"] = stack.enter_context(
            patch("include.ingestion.scripts.sitadel.fetch_dido_pages",
                  return_value=iter(pages))
        )
        mocks["load_df"] = stack.enter_context(
            patch("include.ingestion.scripts.sitadel.load_df",
                  side_effect=lambda c, df, t: len(df))
        )
        mocks["set_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.sitadel.set_watermark")
        )
        result = run(full_refresh=full_refresh, dry_run=dry_run)

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
# _parse_watermark
# ---------------------------------------------------------------------------

class TestParseWatermark:
    def test_valid(self):
        assert _parse_watermark("2024-03") == (2024, 3)

    def test_none_returns_none(self):
        assert _parse_watermark(None) is None

    def test_invalid_returns_none(self):
        assert _parse_watermark("bad") is None


# ---------------------------------------------------------------------------
# parse
# ---------------------------------------------------------------------------

class TestParse:
    def test_returns_dataframe(self):
        assert isinstance(parse(_records()), pd.DataFrame)

    def test_empty_input(self):
        assert parse([]).empty

    def test_renames_annee(self):
        df = parse(_records(ANNEE="2023"))
        assert "annee" in df.columns
        assert df["annee"].iloc[0] == 2023

    def test_renames_mois(self):
        df = parse(_records(MOIS="7"))
        assert df["mois"].iloc[0] == 7

    def test_renames_code_commune(self):
        df = parse(_records(CODE_INSEE="13055"))
        assert df["code_commune"].iloc[0] == "13055"

    def test_derives_code_departement(self):
        df = parse(_records(CODE_INSEE="13055"))
        assert df["code_departement"].iloc[0] == "13"

    def test_derives_dept_dom(self):
        df = parse(_records(CODE_INSEE="97200"))
        assert df["code_departement"].iloc[0] == "972"

    def test_renames_type_logement(self):
        df = parse(_records(TYPE_LGT="Collectif"))
        assert df["type_logement"].iloc[0] == "Collectif"

    def test_renames_log_aut(self):
        df = parse(_records(LOG_AUT=10))
        assert df["nb_logements_autorises"].iloc[0] == 10

    def test_renames_log_com(self):
        df = parse(_records(LOG_COM=7))
        assert df["nb_logements_commences"].iloc[0] == 7

    def test_renames_sdp_aut(self):
        df = parse(_records(SDP_AUT=900))
        assert df["surface_autorisee"].iloc[0] == pytest.approx(900.0)

    def test_renames_sdp_com(self):
        df = parse(_records(SDP_COM=600))
        assert df["surface_commencee"].iloc[0] == pytest.approx(600.0)

    def test_null_log_aut_becomes_na(self):
        df = parse(_records(LOG_AUT=None))
        assert pd.isna(df["nb_logements_autorises"].iloc[0])

    def test_unknown_dido_columns_dropped(self):
        recs = _records()
        recs[0]["PTM2_MOY"] = "999"
        df = parse(recs)
        assert "PTM2_MOY" not in df.columns


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

class TestRun:
    def test_creates_table_on_startup(self):
        _, mocks = _run_mocked()
        sql = mocks["client"].command.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert TABLE in sql

    def test_passes_order_by_to_fetch(self):
        _, mocks = _run_mocked()
        call_params = mocks["fetch_dido_pages"].call_args[0][1]
        assert "orderBy" in call_params
        assert call_params["orderBy"] == "-ANNEE,-MOIS"

    def test_full_refresh_skips_watermark_read(self):
        _, mocks = _run_mocked(full_refresh=True)
        mocks["get_watermark"].assert_not_called()

    def test_incremental_reads_watermark(self):
        _, mocks = _run_mocked(watermark="2023-12")
        mocks["get_watermark"].assert_called_once_with(mocks["client"], SOURCE)

    def test_dry_run_skips_insert_and_watermark(self):
        result, mocks = _run_mocked(dry_run=True)
        assert result == 0
        mocks["load_df"].assert_not_called()
        mocks["set_watermark"].assert_not_called()

    def test_stops_early_when_page_is_old(self):
        """Pagination stops when a page contains rows older than watermark."""
        new_page = _records(ANNEE="2024", MOIS="03")
        old_page = _records(ANNEE="2023", MOIS="06")
        _, mocks = _run_mocked(watermark="2024-01", pages=[new_page, old_page])
        assert mocks["load_df"].call_count == 1

    def test_watermark_set_to_max_annee_mois(self):
        pages = [
            _records(ANNEE="2024", MOIS="11"),
            _records(ANNEE="2024", MOIS="03"),
        ]
        _, mocks = _run_mocked(pages=pages)
        mocks["set_watermark"].assert_called_once_with(
            mocks["client"], SOURCE, "2024-11"
        )

    def test_returns_row_count(self):
        result, _ = _run_mocked()
        assert result == 1

    def test_no_data_no_watermark_set(self):
        _, mocks = _run_mocked(pages=[[]])
        mocks["set_watermark"].assert_not_called()
