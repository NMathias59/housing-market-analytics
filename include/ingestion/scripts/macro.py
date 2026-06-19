"""
Ingestion incrémentale Macro — Taux immobiliers (Banque de France).

Source    : Banque de France / fichier CSV série temporelle mensuelle
Table     : db_wh_logement_raw.raw_macro_taux
Watermark : date (dernière période chargée au format YYYY-MM, ex. "2024-11")

Le fichier est petit (~300 lignes). Le filtrage incrémental se fait côté client
dans parse() : seules les lignes dont la date > watermark sont insérées.

Env vars requis :
    CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
    MACRO_BDF_URL — URL du fichier CSV BdF (historique complet)

Usage :
    python -m include.ingestion.scripts.macro
    python -m include.ingestion.scripts.macro --full-refresh
    python -m include.ingestion.scripts.macro --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os

import pandas as pd
from dotenv import load_dotenv

from include.ingestion.base import (
    ensure_watermark_table,
    get_client,
    get_watermark,
    iter_csv_chunks,
    load_df,
    set_watermark,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SOURCE = "macro_taux"
TABLE = "db_wh_logement_raw.raw_macro_taux"

DDL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE}
    (
        date                     Date,
        annee                    UInt16,
        mois                     UInt8,
        taux_credit_immobilier   Nullable(Float64),
        taux_oat_10ans           Nullable(Float64),
        duree_moyenne_credit     Nullable(Float64),
        taux_inflation           Nullable(Float64),
        _loaded_at               DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(_loaded_at)
    ORDER BY (date)
"""

_FLOAT_COLS = [
    "taux_credit_immobilier",
    "taux_oat_10ans",
    "duree_moyenne_credit",
    "taux_inflation",
]
COLUMNS = ["date"] + _FLOAT_COLS


# ---------------------------------------------------------------------------
# Extract / Transform
# ---------------------------------------------------------------------------

def parse(chunk: pd.DataFrame, since_date: str | None = None) -> pd.DataFrame:
    """
    Cast a raw BdF chunk to the raw_macro_taux schema.

    *since_date* is a YYYY-MM string. Rows with date <= since_date are dropped
    for incremental loading — all filtering happens client-side since the file
    is small and does not support server-side date filters.
    """
    present = [c for c in COLUMNS if c in chunk.columns]
    df = chunk[present].copy()

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], format="mixed", errors="coerce")
        df = df.dropna(subset=["date"])

        if since_date:
            cutoff = pd.to_datetime(since_date, format="%Y-%m")
            df = df[df["date"] > cutoff]

        df["annee"] = df["date"].dt.year.astype("Int64")
        df["mois"] = df["date"].dt.month.astype("Int64")

    for col in _FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].str.replace(",", "."), errors="coerce"
            )

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def run(*, full_refresh: bool = False, dry_run: bool = False) -> int:
    """
    Ingest BdF macro time series incrementally into ClickHouse.

    Downloads the full CSV each run (file is tiny), inserts only rows
    newer than the watermark. Returns the number of rows inserted.
    """
    url = os.environ.get("MACRO_BDF_URL", "")
    if not url:
        raise EnvironmentError(
            "MACRO_BDF_URL is not set. "
            "Set it to the Banque de France CSV URL for interest rate series."
        )

    client = get_client()
    ensure_watermark_table(client)
    client.command(DDL)

    watermark = None if full_refresh else get_watermark(client, SOURCE)

    if watermark:
        logger.info("Incremental run — fetching macro taux after %s", watermark)
    else:
        logger.info("Full load — fetching all macro taux history")

    total_inserted = 0
    max_date_seen: pd.Timestamp | None = None

    for chunk in iter_csv_chunks(url):
        df = parse(chunk, since_date=watermark)
        if df.empty:
            continue

        if "date" in df.columns:
            batch_max = df["date"].dropna().max()
            if pd.notna(batch_max):
                if max_date_seen is None or batch_max > max_date_seen:
                    max_date_seen = batch_max

        if dry_run:
            logger.info("[dry-run] Would insert %d rows (up to %s)",
                        len(df), max_date_seen)
        else:
            total_inserted += load_df(client, df, TABLE)

    if not dry_run and max_date_seen is not None:
        set_watermark(client, SOURCE, max_date_seen.strftime("%Y-%m"))

    logger.info("Done — %d rows inserted (dry_run=%s)", total_inserted, dry_run)
    return total_inserted


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest macro taux (Banque de France) into ClickHouse."
    )
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignore the watermark and reload all history.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Download and parse without writing to ClickHouse.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    run(full_refresh=args.full_refresh, dry_run=args.dry_run)
