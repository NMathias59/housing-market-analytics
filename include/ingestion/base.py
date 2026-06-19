"""
Base utilities shared by all ingestion scripts:
  - ClickHouse client factory
  - Watermark table (incremental loading)
  - Paginated REST API fetcher with automatic retry
  - Batch DataFrame loader
"""

from __future__ import annotations

import logging
import os
from typing import Any, Iterator

import pandas as pd
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

RAW_DB = "db_logement_raw"
WATERMARK_TABLE = f"{RAW_DB}._ingestion_watermarks"
DEFAULT_PAGE_SIZE = 1_000


# ---------------------------------------------------------------------------
# ClickHouse client
# ---------------------------------------------------------------------------

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
    retry=retry_if_exception_type(requests.HTTPError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    reraise=True,
)
def _get(url: str, params: dict[str, Any]) -> dict:
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


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


# ---------------------------------------------------------------------------
# Batch loader
# ---------------------------------------------------------------------------

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
