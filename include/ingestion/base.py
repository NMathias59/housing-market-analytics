"""
Base utilities shared by all ingestion scripts:
  - ClickHouse client factory
  - Watermark table (incremental loading)
  - Paginated REST API fetcher with automatic retry
  - Batch DataFrame loader
"""

from __future__ import annotations

import gzip
import logging
import os
from typing import Any, Iterator

import pandas as pd
import requests
from tenacity import (
    retry,
    retry_any,
    retry_if_exception,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

RAW_DB = "db_wh_housing"
WATERMARK_TABLE = f"{RAW_DB}._ingestion_watermarks"
DEFAULT_PAGE_SIZE = 1_000


# ---------------------------------------------------------------------------
# ClickHouse client
# ---------------------------------------------------------------------------

def _is_ch_connection_error(exc: BaseException) -> bool:
    return any("Connection refused" in str(e) for e in (exc, exc.__cause__, exc.__context__) if e)


@retry(
    retry=retry_if_exception(_is_ch_connection_error),
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=2, min=10, max=120),
    reraise=True,
)
def get_client():
    """Return a ClickHouse client configured from environment variables."""
    import clickhouse_connect  # local import — optional dep at module level

    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
    )


# ---------------------------------------------------------------------------
# Watermark table
# ---------------------------------------------------------------------------

def ensure_watermark_table(client) -> None:
    """Create the watermark tracking table if it does not exist."""
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE}
        (
            source     String,
            last_value String,
            updated_at DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (source)
    """)


def get_watermark(client, source: str) -> str | None:
    """
    Return the last loaded value for *source*, or None on first run.

    Uses FINAL to force deduplication by ReplacingMergeTree before reading.
    """
    result = client.query(
        f"SELECT last_value FROM {WATERMARK_TABLE} FINAL WHERE source = %(source)s",
        parameters={"source": source},
    )
    rows = result.result_rows
    return rows[0][0] if rows else None


def set_watermark(client, source: str, value: str) -> None:
    """
    Persist a new watermark for *source*.

    Always call this AFTER a successful insert — never before.
    ReplacingMergeTree deduplicates by (source) on updated_at, so each
    INSERT effectively replaces the previous watermark at merge time.
    """
    client.insert(
        WATERMARK_TABLE,
        [[source, value, pd.Timestamp.now("UTC")]],
        column_names=["source", "last_value", "updated_at"],
    )
    logger.info("Watermark updated: %s → %s", source, value)


# ---------------------------------------------------------------------------
# Paginated REST API fetcher
# ---------------------------------------------------------------------------

@retry(
    retry=retry_any(
        retry_if_exception_type(requests.HTTPError),
        retry_if_exception_type(requests.ConnectionError),
        retry_if_exception_type(requests.Timeout),
    ),
    stop=stop_after_attempt(8),
    wait=wait_exponential(multiplier=2, min=5, max=120),
    reraise=True,
)
def _get(url: str, params: dict[str, Any]) -> dict:
    response = requests.get(url, params=params, timeout=90)
    response.raise_for_status()
    return response.json()


def get_json(url: str, params: dict[str, Any] | None = None) -> dict:
    """HTTP GET with automatic retry on 429/5xx, returns parsed JSON."""
    return _get(url, params or {})


def fetch_pages(
    url: str,
    params: dict[str, Any],
    *,
    data_key: str = "data",
    total_key: str = "total",
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Iterator[list[dict]]:
    """
    Yield successive pages of records from a paginated REST API.

    Stops when the API returns an empty page or when the running offset
    reaches the total reported by the response envelope.

    Args:
        url:       Endpoint URL.
        params:    Base query parameters (watermark filters go here).
        data_key:  JSON key that holds the list of records.
        total_key: JSON key that holds the total record count.
        page_size: Number of records to request per page.
    """
    offset = 0
    while True:
        payload = _get(url, {**params, "offset": offset, "limit": page_size})
        records = payload.get(data_key, [])
        if not records:
            break
        yield records
        total = payload.get(total_key, float("inf"))
        offset += page_size
        logger.debug("Fetched %d / %s records from %s", offset, total, url)
        if offset >= total:
            break


def fetch_dido_pages(
    url: str,
    params: dict[str, Any],
    *,
    page_size: int = 100,
) -> Iterator[list[dict]]:
    """
    Yield pages from a DiDo API endpoint (1-indexed page-based pagination).

    DiDo response envelope: {"data": [...], "totalCount": N}
    pageSize must be one of 10, 20, 50, 100 (API constraint).
    Stops when an empty page is received or all pages have been fetched.
    """
    page = 1
    while True:
        payload = _get(url, {**params, "page": page, "pageSize": page_size})
        records = payload.get("data", [])
        if not records:
            break
        yield records
        total = payload.get("total", 0)
        if page * page_size >= total:
            break
        page += 1
        logger.debug("DiDo page %d / %d from %s", page, -(-total // page_size), url)


def fetch_ademe_pages(
    url: str,
    params: dict[str, Any],
    *,
    page_size: int = 10_000,
) -> Iterator[list[dict]]:
    """
    Yield pages from an ADEME data-fair REST API endpoint.

    ADEME response envelope: {"total": N, "results": [...], "next": "<url>"}
    Uses cursor-based pagination via the "next" URL to avoid the 10 000-row
    offset ceiling that the API imposes on page-based pagination.
    """
    current_url: str = url
    current_params: dict[str, Any] = {**params, "size": page_size}
    page = 0
    while True:
        payload = _get(current_url, current_params)
        records = payload.get("results", [])
        if not records:
            break
        page += 1
        yield records
        next_url: str | None = payload.get("next")
        if not next_url:
            break
        # All query params are already encoded in the cursor URL.
        current_url = next_url
        current_params = {}
        logger.debug("ADEME cursor page %d from %s", page + 1, current_url[:80])


# ---------------------------------------------------------------------------
# Batch loader
# ---------------------------------------------------------------------------

def iter_csv_chunks(
    url: str,
    chunk_size: int = DEFAULT_PAGE_SIZE,
    *,
    sep: str = ",",
) -> Iterator[pd.DataFrame]:
    """
    Stream-download a CSV or CSV.GZ from *url* and yield DataFrame chunks.

    The file is never fully loaded in memory — gzip is decompressed directly
    on the socket stream. Suitable for files exceeding 1 GB uncompressed.
    """
    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        resp.raw.decode_content = True
        fileobj = gzip.GzipFile(fileobj=resp.raw) if url.endswith(".gz") else resp.raw
        yield from pd.read_csv(
            fileobj,
            chunksize=chunk_size,
            sep=sep,
            dtype=str,
            low_memory=False,
        )


def load_df(client, df: pd.DataFrame, table: str) -> int:
    """
    Insert *df* into *table* and return the number of rows inserted.

    Adds a _loaded_at timestamp column used by dbt's argMax deduplication
    pattern in the staging layer.
    """
    if df.empty:
        logger.warning("Empty DataFrame — skipping insert into %s", table)
        return 0
    df = df.copy()
    df["_loaded_at"] = pd.Timestamp.now("UTC")
    client.insert_df(table, df)
    logger.info("Inserted %d rows into %s", len(df), table)
    return len(df)
