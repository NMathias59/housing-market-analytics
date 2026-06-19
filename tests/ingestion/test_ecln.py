"""Unit tests for include/ingestion/scripts/ecln.py"""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from include.ingestion.scripts.ecln import (
    SOURCE, TABLE,
    _parse_trimestre, parse, run,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _records(**overrides) -> list[dict]:
    base = {
        "TRIMESTRE": "2024-T3",
        "DEP_CODE": "75",
        "DEP_LIBELLE": "Paris",
        "TYPE_LGT": "Collectif",
        "MEV": 120,
        "RESA": 95,
        "ANNUL": 5,
        "STOCK": 300,
        "DELAI_ECOUL": 4.5,
        "PRIX_M2": 9800,
        "PRIX_MOY_IND": None,
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
            patch("include.ingestion.scripts.ecln.get_client", return_value=client)
        )
        stack.enter_context(
            patch("include.ingestion.scripts.ecln.ensure_watermark_table")
        )
        mocks["get_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.ecln.get_watermark",
                  return_value=watermark)
        )
        mocks["fetch_dido_pages"] = stack.enter_context(
            patch("include.ingestion.scripts.ecln.fetch_dido_pages",
                  return_value=iter(pages))
        )
        mocks["load_df"] = stack.enter_context(
            patch("include.ingestion.scripts.ecln.load_df",
                  side_effect=lambda c, df, t: len(df))
        )
        mocks["set_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.ecln.set_watermark")
        )
        result = run(full_refresh=full_refresh, dry_run=dry_run)

    return result, mocks


# ---------------------------------------------------------------------------
# _parse_trimestre
# ---------------------------------------------------------------------------

class TestParseTrimestre:
    def test_valid(self):
        assert _parse_trimestre("2024-T3") == (2024, 3)

    def test_q1(self):
        assert _parse_trimestre("2020-T1") == (2020, 1)

    def test_invalid_returns_nones(self):
        assert _parse_trimestre("bad") == (None, None)

    def test_none_input_returns_nones(self):
        assert _parse_trimestre(None) == (None, None)


# ---------------------------------------------------------------------------
# parse
# ---------------------------------------------------------------------------

class TestParse:
    def test_returns_dataframe(self):
        assert isinstance(parse(_records()), pd.DataFrame)

    def test_empty_input(self):
        assert parse([]).empty

    def test_derives_annee_from_trimestre(self):
        df = parse(_records(TRIMESTRE="2023-T2"))
        assert df["annee"].iloc[0] == 2023

    def test_derives_trimestre_number(self):
        df = parse(_records(TRIMESTRE="2023-T2"))
        assert df["trimestre"].iloc[0] == 2

    def test_renames_dep_code(self):
        df = parse(_records(DEP_CODE="69"))
        assert df["code_departement"].iloc[0] == "69"

    def test_renames_mev(self):
        df = parse(_records(MEV=200))
        assert df["nb_mises_en_vente"].iloc[0] == 200

    def test_renames_resa(self):
        df = parse(_records(RESA=180))
        assert df["nb_reservations"].iloc[0] == 180

    def test_renames_annul(self):
        df = parse(_records(ANNUL=10))
        assert df["nb_annulations"].iloc[0] == 10

    def test_renames_prix_m2(self):
        df = parse(_records(PRIX_M2=4500))
        assert df["prix_m2"].iloc[0] == pytest.approx(4500.0)

    def test_null_prix_becomes_nan(self):
        df = parse(_records(PRIX_MOY_IND=None))
        assert pd.isna(df["prix_moyen_individuel"].iloc[0])

    def test_since_trimestre_filters_old_rows(self):
        records = [
            {**_records(TRIMESTRE="2024-T1")[0]},
            {**_records(TRIMESTRE="2023-T4")[0]},
        ]
        df = parse(records, since_trimestre="2023-T4")
        assert len(df) == 1
        assert df["annee"].iloc[0] == 2024

    def test_no_filter_returns_all(self):
        records = [
            {**_records(TRIMESTRE="2024-T1")[0]},
            {**_records(TRIMESTRE="2023-T4")[0]},
        ]
        df = parse(records, since_trimestre=None)
        assert len(df) == 2

    def test_trimestre_raw_not_in_output(self):
        df = parse(_records())
        assert "TRIMESTRE" not in df.columns

    def test_unknown_columns_dropped(self):
        recs = _records()
        recs[0]["CHAMP_INCONNU"] = "x"
        df = parse(recs)
        assert "CHAMP_INCONNU" not in df.columns


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

    def test_incremental_reads_watermark(self):
        _, mocks = _run_mocked(watermark="2023-T4")
        mocks["get_watermark"].assert_called_once_with(mocks["client"], SOURCE)

    def test_dry_run_skips_insert_and_watermark(self):
        result, mocks = _run_mocked(dry_run=True)
        assert result == 0
        mocks["load_df"].assert_not_called()
        mocks["set_watermark"].assert_not_called()

    def test_watermark_set_to_max_trimestre(self):
        pages = [
            _records(TRIMESTRE="2024-T3"),
            _records(TRIMESTRE="2024-T1"),
        ]
        _, mocks = _run_mocked(pages=pages)
        mocks["set_watermark"].assert_called_once_with(
            mocks["client"], SOURCE, "2024-T3"
        )

    def test_old_rows_filtered_when_incremental(self):
        pages = [
            _records(TRIMESTRE="2024-T1"),
            _records(TRIMESTRE="2023-T4"),
        ]
        result, mocks = _run_mocked(watermark="2023-T4", pages=pages)
        assert result == 1

    def test_returns_row_count(self):
        result, _ = _run_mocked()
        assert result == 1

    def test_empty_after_filter_skips_watermark(self):
        pages = [_records(TRIMESTRE="2023-T2")]
        _, mocks = _run_mocked(watermark="2024-T1", pages=pages)
        mocks["set_watermark"].assert_not_called()
