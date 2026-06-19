"""Unit tests for include/ingestion/scripts/ecln.py"""

from __future__ import annotations

import os
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from include.ingestion.scripts.ecln import SOURCE, TABLE, build_params, parse, run

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_RECORDS = [
    {
        "annee": "2022",
        "trimestre": "3",
        "code_departement": "75",
        "code_region": "11",
        "type_logement": "collectif",
        "nb_logements_vendus": "1250",
        "nb_logements_mis_en_vente": "1400",
        "stock": "3200",
        "prix_moyen_m2": "6800.0",
        "surface_moyenne": "65.5",
        "delai_commercialisation": "8.2",
    },
    {
        "annee": "2023",
        "trimestre": "1",
        "code_departement": "69",
        "code_region": "84",
        "type_logement": "individuel",
        "nb_logements_vendus": "420",
        "nb_logements_mis_en_vente": "500",
        "stock": "980",
        "prix_moyen_m2": "3200.0",
        "surface_moyenne": "95.0",
        "delai_commercialisation": "12.5",
    },
]


def _run_mocked(
    *,
    watermark: str | None = None,
    records: list | None = None,
    full_refresh: bool = False,
    dry_run: bool = False,
    rid: str = "fake-ecln-rid",
):
    if records is None:
        records = SAMPLE_RECORDS
    client = MagicMock()
    mocks: dict = {"client": client}

    with ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, {"ECLN_DIDO_RID": rid}))
        stack.enter_context(
            patch("include.ingestion.scripts.ecln.get_client", return_value=client)
        )
        stack.enter_context(patch("include.ingestion.scripts.ecln.ensure_watermark_table"))
        mocks["get_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.ecln.get_watermark", return_value=watermark)
        )
        mocks["fetch_dido_pages"] = stack.enter_context(
            patch("include.ingestion.scripts.ecln.fetch_dido_pages",
                  return_value=iter([records]))
        )
        mocks["load_df"] = stack.enter_context(
            patch("include.ingestion.scripts.ecln.load_df", return_value=len(records))
        )
        mocks["set_watermark"] = stack.enter_context(
            patch("include.ingestion.scripts.ecln.set_watermark")
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
            "annee": "2022", "trimestre": "2",
            "code_departement": "75", "code_region": "11",
            "type_logement": "collectif",
            "nb_logements_vendus": "500", "nb_logements_mis_en_vente": "600",
            "stock": "1500", "prix_moyen_m2": "5000.0",
            "surface_moyenne": "60.0", "delai_commercialisation": "9.0",
        }
        base.update(kw)
        return base

    def test_returns_dataframe(self):
        assert isinstance(parse([self._record()]), pd.DataFrame)

    def test_empty_input_returns_empty_df(self):
        assert parse([]).empty

    def test_parses_annee(self):
        df = parse([self._record(annee="2022")])
        assert df["annee"].iloc[0] == 2022

    def test_parses_trimestre_as_int(self):
        df = parse([self._record(trimestre="3")])
        assert df["trimestre"].iloc[0] == 3

    def test_parses_float_col(self):
        df = parse([self._record(prix_moyen_m2="5432.1")])
        assert df["prix_moyen_m2"].iloc[0] == pytest.approx(5432.1)

    def test_invalid_numeric_becomes_nan(self):
        df = parse([self._record(nb_logements_vendus="N/D")])
        assert pd.isna(df["nb_logements_vendus"].iloc[0])

    def test_null_string_becomes_empty(self):
        df = parse([self._record(type_logement=None)])
        assert df["type_logement"].iloc[0] == ""

    def test_unknown_columns_dropped(self):
        df = parse([{**self._record(), "col_extra": "x"}])
        assert "col_extra" not in df.columns

    def test_multiple_records(self):
        assert len(parse(SAMPLE_RECORDS)) == 2


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

class TestRun:
    def test_raises_if_rid_not_set(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("ECLN_DIDO_RID", None)
            with pytest.raises(EnvironmentError, match="ECLN_DIDO_RID"):
                run()

    def test_full_refresh_skips_watermark(self):
        _, mocks = _run_mocked(watermark="2021", full_refresh=True)
        mocks["get_watermark"].assert_not_called()

    def test_incremental_reads_watermark(self):
        _, mocks = _run_mocked(watermark="2021")
        mocks["get_watermark"].assert_called_once_with(mocks["client"], SOURCE)

    def test_incremental_passes_filter(self):
        _, mocks = _run_mocked(watermark="2021")
        params = mocks["fetch_dido_pages"].call_args[0][1]
        assert "2021" in params.get("filters", "")

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
        _, mocks = _run_mocked(rid="ecln-rid-xyz")
        endpoint = mocks["fetch_dido_pages"].call_args[0][0]
        assert "ecln-rid-xyz" in endpoint
