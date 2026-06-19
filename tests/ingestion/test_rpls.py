"""Unit tests for include/ingestion/scripts/rpls.py"""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from include.ingestion.scripts.rpls import (
    SOURCE, TABLE,
    get_millesimes, parse, run,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _records(**overrides) -> list[dict]:
    base = {
        "DEPCOM":     "75056",
        "DEP_CODE":   "75",
        "REG_CODE":   "11",
        "FINAN_CODE": "SAN",
        "DPEENERGIE": "C",
        "CONSTRUCT":  "1985",
        "SURFHAB":    48.5,
        "NBPIECE":    2,
    }
    base.update(overrides)
    return [base]


def _run_mocked(
    *,
    watermark: str | None = None,
    millesimes: list[str] | None = None,
    chunks: list[list[dict]] | None = None,
    full_refresh: bool = False,
    dry_run: bool = False,
):
    if millesimes is None:
        millesimes = ["2024-01"]
    if chunks is None:
        chunks = [_records()]

    client = MagicMock()
    mocks: dict = {"client": client}

    with ExitStack() as stack:
        stack.enter_context(
            patch("include.ingestion.scripts.rpls.get_client", return_value=client)
        )
        stack.enter_context(
            patch("include.ingestion.scripts.rpls.ensure_watermark_table")
        )
        mocks["get_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.rpls.get_watermark",
                  return_value=watermark)
        )
        mocks["get_millesimes"] = stack.enter_context(
            patch("include.ingestion.scripts.rpls.get_millesimes",
                  return_value=millesimes)
        )
        mocks["iter_csv_chunks"] = stack.enter_context(
            patch("include.ingestion.scripts.rpls.iter_csv_chunks",
                  side_effect=lambda url, **kw: iter(
                      pd.DataFrame(c) for c in chunks
                  ))
        )
        mocks["load_df"] = stack.enter_context(
            patch("include.ingestion.scripts.rpls.load_df",
                  side_effect=lambda c, df, t: len(df))
        )
        mocks["set_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.rpls.set_watermark")
        )
        result = run(full_refresh=full_refresh, dry_run=dry_run)

    return result, mocks


# ---------------------------------------------------------------------------
# get_millesimes
# ---------------------------------------------------------------------------

class TestGetMillesimes:
    def test_returns_sorted_list(self):
        fake_data = {
            "datafiles": [{
                "rid": "f3c2f2cb-8fb1-40fd-8733-964247744c9a",
                "millesimes": [
                    {"millesime": "2024-01"},
                    {"millesime": "2022-01"},
                    {"millesime": "2023-01"},
                ],
            }]
        }
        with patch("include.ingestion.scripts.rpls.requests.get") as mock_get:
            mock_get.return_value.json.return_value = fake_data
            mock_get.return_value.raise_for_status = lambda: None
            mils = get_millesimes()
        assert mils == ["2022-01", "2023-01", "2024-01"]

    def test_returns_empty_when_rid_not_found(self):
        with patch("include.ingestion.scripts.rpls.requests.get") as mock_get:
            mock_get.return_value.json.return_value = {"datafiles": []}
            mock_get.return_value.raise_for_status = lambda: None
            mils = get_millesimes()
        assert mils == []


# ---------------------------------------------------------------------------
# parse
# ---------------------------------------------------------------------------

class TestParse:
    def test_returns_dataframe(self):
        assert isinstance(parse(_records(), 2024), pd.DataFrame)

    def test_empty_input(self):
        assert parse([], 2024).empty

    def test_adds_annee_from_millesime(self):
        df = parse(_records(), 2024)
        assert df["annee"].iloc[0] == 2024

    def test_renames_depcom(self):
        df = parse(_records(DEPCOM="69001"), 2024)
        assert df["code_commune"].iloc[0] == "69001"

    def test_renames_dep_code(self):
        df = parse(_records(DEP_CODE="69"), 2024)
        assert df["code_departement"].iloc[0] == "69"

    def test_renames_reg_code(self):
        df = parse(_records(REG_CODE="84"), 2024)
        assert df["code_region"].iloc[0] == "84"

    def test_renames_finan_code(self):
        df = parse(_records(FINAN_CODE="52"), 2024)
        assert df["financement"].iloc[0] == "52"

    def test_renames_dpeenergie(self):
        df = parse(_records(DPEENERGIE="B"), 2024)
        assert df["classe_energie_dpe"].iloc[0] == "B"

    def test_renames_construct_as_int(self):
        df = parse(_records(CONSTRUCT="1975"), 2024)
        assert df["annee_construction"].iloc[0] == 1975

    def test_renames_surfhab(self):
        df = parse(_records(SURFHAB=72.5), 2024)
        assert df["surface"].iloc[0] == pytest.approx(72.5)

    def test_renames_nbpiece(self):
        df = parse(_records(NBPIECE=3), 2024)
        assert df["nb_pieces"].iloc[0] == 3

    def test_null_depcom_becomes_empty_string(self):
        df = parse(_records(DEPCOM=None), 2024)
        assert df["code_commune"].iloc[0] == ""

    def test_invalid_construct_becomes_na(self):
        df = parse(_records(CONSTRUCT="NC"), 2024)
        assert pd.isna(df["annee_construction"].iloc[0])

    def test_no_loyer_column(self):
        df = parse(_records(), 2024)
        assert "loyer" not in df.columns

    def test_no_taux_occupation_column(self):
        df = parse(_records(), 2024)
        assert "taux_occupation" not in df.columns

    def test_unknown_columns_dropped(self):
        recs = _records()
        recs[0]["NOMVOIE"] = "Rue de la Paix"
        df = parse(recs, 2024)
        assert "NOMVOIE" not in df.columns


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
        _, mocks = _run_mocked(watermark="2023-01")
        mocks["get_watermark"].assert_called_once_with(mocks["client"], SOURCE)

    def test_skips_millesimes_before_watermark(self):
        _, mocks = _run_mocked(
            watermark="2023-01",
            millesimes=["2022-01", "2023-01", "2024-01"],
        )
        wm_calls = [c[0][2] for c in mocks["set_watermark"].call_args_list]
        assert "2022-01" not in wm_calls
        assert "2023-01" not in wm_calls
        assert "2024-01" in wm_calls

    def test_already_up_to_date_inserts_nothing(self):
        result, mocks = _run_mocked(
            watermark="2024-01", millesimes=["2024-01"]
        )
        assert result == 0
        mocks["load_df"].assert_not_called()

    def test_dry_run_skips_insert_and_watermark(self):
        result, mocks = _run_mocked(dry_run=True)
        assert result == 0
        mocks["load_df"].assert_not_called()
        mocks["set_watermark"].assert_not_called()

    def test_watermark_set_per_millesime(self):
        _, mocks = _run_mocked(
            millesimes=["2023-01", "2024-01"],
            chunks=[_records(), _records()],
        )
        wm_calls = [c[0][2] for c in mocks["set_watermark"].call_args_list]
        assert "2023-01" in wm_calls
        assert "2024-01" in wm_calls

    def test_annee_derived_from_millesime(self):
        """Rows inserted should have annee = int(millesime[:4])."""
        captured_dfs: list = []

        def capture(client, df, table):
            captured_dfs.append(df.copy())
            return len(df)

        with ExitStack() as stack:
            stack.enter_context(
                patch("include.ingestion.scripts.rpls.get_client",
                      return_value=MagicMock())
            )
            stack.enter_context(
                patch("include.ingestion.scripts.rpls.ensure_watermark_table")
            )
            stack.enter_context(
                patch("include.ingestion.scripts.rpls.get_watermark",
                      return_value=None)
            )
            stack.enter_context(
                patch("include.ingestion.scripts.rpls.get_millesimes",
                      return_value=["2024-01"])
            )
            stack.enter_context(
                patch("include.ingestion.scripts.rpls.iter_csv_chunks",
                      side_effect=lambda url, **kw: iter([pd.DataFrame(_records())]))
            )
            stack.enter_context(
                patch("include.ingestion.scripts.rpls.load_df",
                      side_effect=capture)
            )
            stack.enter_context(
                patch("include.ingestion.scripts.rpls.set_watermark")
            )
            run()

        assert captured_dfs[0]["annee"].iloc[0] == 2024

    def test_returns_total_rows(self):
        result, _ = _run_mocked()
        assert result == 1
