"""Unit tests for include/ingestion/base.py"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from include.ingestion.base import (
    WATERMARK_TABLE,
    ensure_watermark_table,
    fetch_dido_pages,
    fetch_pages,
    get_watermark,
    load_df,
    set_watermark,
)


# ---------------------------------------------------------------------------
# ensure_watermark_table
# ---------------------------------------------------------------------------

class TestEnsureWatermarkTable:
    def test_calls_command_once(self):
        client = MagicMock()
        ensure_watermark_table(client)
        client.command.assert_called_once()

    def test_sql_contains_create_if_not_exists(self):
        client = MagicMock()
        ensure_watermark_table(client)
        sql = client.command.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql

    def test_sql_targets_correct_table(self):
        client = MagicMock()
        ensure_watermark_table(client)
        sql = client.command.call_args[0][0]
        assert WATERMARK_TABLE in sql

    def test_sql_uses_replacing_merge_tree(self):
        client = MagicMock()
        ensure_watermark_table(client)
        sql = client.command.call_args[0][0]
        assert "ReplacingMergeTree" in sql

    def test_sql_orders_by_source(self):
        client = MagicMock()
        ensure_watermark_table(client)
        sql = client.command.call_args[0][0]
        assert "ORDER BY" in sql
        assert "source" in sql


# ---------------------------------------------------------------------------
# get_watermark
# ---------------------------------------------------------------------------

class TestGetWatermark:
    def test_returns_none_on_first_run(self):
        client = MagicMock()
        client.query.return_value.result_rows = []
        assert get_watermark(client, "eptb") is None

    def test_returns_stored_value(self):
        client = MagicMock()
        client.query.return_value.result_rows = [("2023-01-01",)]
        assert get_watermark(client, "eptb") == "2023-01-01"

    def test_query_uses_final_modifier(self):
        client = MagicMock()
        client.query.return_value.result_rows = []
        get_watermark(client, "eptb")
        sql = client.query.call_args[0][0]
        assert "FINAL" in sql

    def test_query_filters_by_source(self):
        client = MagicMock()
        client.query.return_value.result_rows = []
        get_watermark(client, "eptb")
        params = client.query.call_args[1]["parameters"]
        assert params["source"] == "eptb"

    def test_different_sources_are_independent(self):
        client = MagicMock()
        client.query.return_value.result_rows = [("2024-06-01",)]
        get_watermark(client, "ecln")
        params = client.query.call_args[1]["parameters"]
        assert params["source"] == "ecln"


# ---------------------------------------------------------------------------
# set_watermark
# ---------------------------------------------------------------------------

class TestSetWatermark:
    def test_calls_insert_once(self):
        client = MagicMock()
        set_watermark(client, "eptb", "2024-01-01")
        client.insert.assert_called_once()

    def test_inserts_into_watermark_table(self):
        client = MagicMock()
        set_watermark(client, "eptb", "2024-01-01")
        table_arg = client.insert.call_args[0][0]
        assert table_arg == WATERMARK_TABLE

    def test_row_contains_source_and_value(self):
        client = MagicMock()
        set_watermark(client, "eptb", "2024-01-01")
        row = client.insert.call_args[0][1][0]
        assert row[0] == "eptb"
        assert row[1] == "2024-01-01"

    def test_row_contains_timestamp(self):
        client = MagicMock()
        set_watermark(client, "eptb", "2024-01-01")
        row = client.insert.call_args[0][1][0]
        assert isinstance(row[2], pd.Timestamp)

    def test_column_names_are_correct(self):
        client = MagicMock()
        set_watermark(client, "eptb", "2024-01-01")
        col_names = client.insert.call_args[1]["column_names"]
        assert col_names == ["source", "last_value", "updated_at"]


# ---------------------------------------------------------------------------
# fetch_pages
# ---------------------------------------------------------------------------

class TestFetchPages:
    def _payload(self, records: list, total: int) -> dict:
        return {"data": records, "total": total}

    @patch("include.ingestion.base._get")
    def test_single_full_page(self, mock_get):
        records = [{"id": 1}, {"id": 2}]
        mock_get.return_value = self._payload(records, 2)
        pages = list(fetch_pages("http://api/data", {}, page_size=100))
        assert pages == [records]
        mock_get.assert_called_once()

    @patch("include.ingestion.base._get")
    def test_multiple_pages(self, mock_get):
        page1 = [{"id": i} for i in range(3)]
        page2 = [{"id": i} for i in range(3, 5)]
        mock_get.side_effect = [
            self._payload(page1, 5),
            self._payload(page2, 5),
        ]
        pages = list(fetch_pages("http://api/data", {}, page_size=3))
        assert pages == [page1, page2]

    @patch("include.ingestion.base._get")
    def test_stops_on_empty_data(self, mock_get):
        mock_get.return_value = self._payload([], 0)
        pages = list(fetch_pages("http://api/data", {}))
        assert pages == []
        mock_get.assert_called_once()

    @patch("include.ingestion.base._get")
    def test_stops_when_offset_reaches_total(self, mock_get):
        records = [{"id": i} for i in range(3)]
        mock_get.return_value = self._payload(records, 3)
        pages = list(fetch_pages("http://api/data", {}, page_size=3))
        assert len(pages) == 1
        mock_get.assert_called_once()

    @patch("include.ingestion.base._get")
    def test_passes_base_params_to_api(self, mock_get):
        mock_get.return_value = self._payload([], 0)
        list(fetch_pages("http://api/data", {"annee": "2023"}))
        call_params = mock_get.call_args[0][1]
        assert call_params["annee"] == "2023"

    @patch("include.ingestion.base._get")
    def test_passes_offset_and_limit(self, mock_get):
        mock_get.return_value = self._payload([], 0)
        list(fetch_pages("http://api/data", {}, page_size=500))
        call_params = mock_get.call_args[0][1]
        assert call_params["limit"] == 500
        assert call_params["offset"] == 0

    @patch("include.ingestion.base._get")
    def test_offset_increments_between_pages(self, mock_get):
        page1 = [{"id": i} for i in range(2)]
        page2 = [{"id": i} for i in range(2, 4)]
        mock_get.side_effect = [
            self._payload(page1, 4),
            self._payload(page2, 4),
        ]
        list(fetch_pages("http://api/data", {}, page_size=2))
        offsets = [c[0][1]["offset"] for c in mock_get.call_args_list]
        assert offsets == [0, 2]

    @patch("include.ingestion.base._get")
    def test_custom_data_key(self, mock_get):
        records = [{"id": 1}]
        mock_get.return_value = {"results": records, "total": 1}
        pages = list(fetch_pages("http://api/data", {}, data_key="results"))
        assert pages == [records]

    @patch("time.sleep")
    @patch("requests.get")
    def test_retries_five_times_on_http_error(self, mock_get, mock_sleep):
        error_response = MagicMock()
        error_response.raise_for_status.side_effect = requests.HTTPError("503")
        mock_get.return_value = error_response
        with pytest.raises(requests.HTTPError):
            list(fetch_pages("http://api/data", {}))
        assert mock_get.call_count == 5

    @patch("time.sleep")
    @patch("requests.get")
    def test_succeeds_after_transient_error(self, mock_get, mock_sleep):
        error_response = MagicMock()
        error_response.raise_for_status.side_effect = requests.HTTPError("503")
        success_response = MagicMock()
        success_response.raise_for_status.return_value = None
        success_response.json.return_value = {"data": [{"id": 1}], "total": 1}
        mock_get.side_effect = [error_response, success_response]
        pages = list(fetch_pages("http://api/data", {}))
        assert pages == [[{"id": 1}]]
        assert mock_get.call_count == 2


# ---------------------------------------------------------------------------
# fetch_dido_pages  (DiDo: page/pageSize/totalCount)
# ---------------------------------------------------------------------------

class TestFetchDidoPages:
    def _payload(self, records: list, total: int) -> dict:
        return {"data": records, "totalCount": total}

    @patch("include.ingestion.base._get")
    def test_single_page(self, mock_get):
        records = [{"id": 1}, {"id": 2}]
        mock_get.return_value = self._payload(records, 2)
        pages = list(fetch_dido_pages("http://dido/rows", {}, page_size=100))
        assert pages == [records]
        mock_get.assert_called_once()

    @patch("include.ingestion.base._get")
    def test_multiple_pages(self, mock_get):
        page1 = [{"id": i} for i in range(3)]
        page2 = [{"id": i} for i in range(3, 5)]
        mock_get.side_effect = [
            self._payload(page1, 5),
            self._payload(page2, 5),
        ]
        pages = list(fetch_dido_pages("http://dido/rows", {}, page_size=3))
        assert pages == [page1, page2]

    @patch("include.ingestion.base._get")
    def test_stops_on_empty_data(self, mock_get):
        mock_get.return_value = self._payload([], 0)
        pages = list(fetch_dido_pages("http://dido/rows", {}))
        assert pages == []
        mock_get.assert_called_once()

    @patch("include.ingestion.base._get")
    def test_stops_when_page_covers_total(self, mock_get):
        records = [{"id": i} for i in range(3)]
        mock_get.return_value = self._payload(records, 3)
        pages = list(fetch_dido_pages("http://dido/rows", {}, page_size=3))
        assert len(pages) == 1
        mock_get.assert_called_once()

    @patch("include.ingestion.base._get")
    def test_uses_page_and_pagesize_params(self, mock_get):
        mock_get.return_value = self._payload([], 0)
        list(fetch_dido_pages("http://dido/rows", {"filters": "annee:gt:2020"}, page_size=500))
        call_params = mock_get.call_args[0][1]
        assert call_params["page"] == 1
        assert call_params["pageSize"] == 500
        assert call_params["filters"] == "annee:gt:2020"

    @patch("include.ingestion.base._get")
    def test_page_increments_between_pages(self, mock_get):
        page1 = [{"id": i} for i in range(2)]
        page2 = [{"id": i} for i in range(2, 4)]
        mock_get.side_effect = [
            self._payload(page1, 4),
            self._payload(page2, 4),
        ]
        list(fetch_dido_pages("http://dido/rows", {}, page_size=2))
        pages_requested = [c[0][1]["page"] for c in mock_get.call_args_list]
        assert pages_requested == [1, 2]

    @patch("include.ingestion.base._get")
    def test_passes_base_params_to_all_pages(self, mock_get):
        page1 = [{"id": 1}]
        page2 = [{"id": 2}]
        mock_get.side_effect = [
            self._payload(page1, 2),
            self._payload(page2, 2),
        ]
        list(fetch_dido_pages("http://dido/rows", {"filters": "annee:gt:2022"}, page_size=1))
        for c in mock_get.call_args_list:
            assert c[0][1]["filters"] == "annee:gt:2022"


# ---------------------------------------------------------------------------
# load_df
# ---------------------------------------------------------------------------

class TestLoadDf:
    def test_returns_row_count(self):
        client = MagicMock()
        df = pd.DataFrame({"id": [1, 2, 3]})
        assert load_df(client, df, "db_logement_raw.raw_eptb") == 3

    def test_calls_insert_df(self):
        client = MagicMock()
        df = pd.DataFrame({"id": [1]})
        load_df(client, df, "db_logement_raw.raw_eptb")
        client.insert_df.assert_called_once()

    def test_passes_correct_table_name(self):
        client = MagicMock()
        df = pd.DataFrame({"id": [1]})
        load_df(client, df, "db_logement_raw.raw_eptb")
        table_arg = client.insert_df.call_args[0][0]
        assert table_arg == "db_logement_raw.raw_eptb"

    def test_adds_loaded_at_column(self):
        client = MagicMock()
        df = pd.DataFrame({"id": [1]})
        load_df(client, df, "db_logement_raw.raw_eptb")
        inserted_df = client.insert_df.call_args[0][1]
        assert "_loaded_at" in inserted_df.columns

    def test_loaded_at_is_timestamp(self):
        client = MagicMock()
        df = pd.DataFrame({"id": [1]})
        load_df(client, df, "db_logement_raw.raw_eptb")
        inserted_df = client.insert_df.call_args[0][1]
        assert pd.api.types.is_datetime64_any_dtype(inserted_df["_loaded_at"])

    def test_does_not_mutate_original_df(self):
        client = MagicMock()
        df = pd.DataFrame({"id": [1]})
        original_cols = list(df.columns)
        load_df(client, df, "db_logement_raw.raw_eptb")
        assert list(df.columns) == original_cols

    def test_skips_insert_on_empty_df(self):
        client = MagicMock()
        result = load_df(client, pd.DataFrame(), "db_logement_raw.raw_eptb")
        assert result == 0
        client.insert_df.assert_not_called()
