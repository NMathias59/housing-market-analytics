"""
Ingestion incrémentale Sit@del2 — Autorisations et mises en chantier.

Source    : SDES / API DiDo v1
Table     : db_logement_raw.raw_sitadel
Watermark : annee (dernière année chargée)

Env vars requis :
    CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
    SITADEL_DIDO_RID  — RID du datafile Sit@del2 dans le catalogue DiDo

Usage :
    python -m include.ingestion.scripts.sitadel
    python -m include.ingestion.scripts.sitadel --full-refresh
    python -m include.ingestion.scripts.sitadel --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os

import pandas as pd
from dotenv import load_dotenv

from include.ingestion.base import (
    ensure_watermark_table,
    fetch_dido_pages,
    get_client,
    get_watermark,
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

SOURCE = "sitadel"
TABLE = "db_logement_raw.raw_sitadel"
_DIDO_BASE = "https://data.statistiques.developpement-durable.gouv.fr/dido/api/v1"

DDL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE}
    (
        annee                         UInt16,
        mois                          UInt8,
        code_commune                  String,
        code_departement              LowCardinality(String),
        type_logement                 LowCardinality(String),
        nb_logements_autorises        Nullable(Int32),
        nb_logements_commences        Nullable(Int32),
        surface_autorisee             Nullable(Float64),
        _loaded_at                    DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(_loaded_at)
    ORDER BY (annee, mois, code_commune, type_logement)
    SETTINGS allow_nullable_key = 1
"""

_INT_COLS = ["mois", "nb_logements_autorises", "nb_logements_commences"]
_FLOAT_COLS = ["surface_autorisee"]
_STRING_COLS = ["code_commune", "code_departement", "type_logement"]
COLUMNS = ["annee"] + _STRING_COLS + _INT_COLS + _FLOAT_COLS


# ---------------------------------------------------------------------------
# Extract / Transform
# ---------------------------------------------------------------------------

def build_params(since_year: int | None) -> dict:
    """Return DiDo API params, filtering on annee > since_year if set."""
    if since_year is not None:
        return {"filters": f"annee:gt:{since_year}"}
    return {}


def parse(records: list[dict]) -> pd.DataFrame:
    """Cast raw DiDo records to the raw_sitadel schema."""
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    present = [c for c in COLUMNS if c in df.columns]
    df = df[present].copy()

    if "annee" in df.columns:
        df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("Int64")

    for col in _INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in _FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in _STRING_COLS:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    return df


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def run(*, full_refresh: bool = False, dry_run: bool = False) -> int:
    """Ingest Sit@del2 data incrementally from DiDo API into ClickHouse."""
    rid = os.environ.get("SITADEL_DIDO_RID", "")
    if not rid:
        raise EnvironmentError(
            "SITADEL_DIDO_RID is not set. "
            "Retrieve the datafile RID from the DiDo catalogue."
        )
    endpoint = f"{_DIDO_BASE}/datafiles/{rid}/rows"

    client = get_client()
    ensure_watermark_table(client)
    client.command(DDL)

    watermark = None if full_refresh else get_watermark(client, SOURCE)
    since_year = int(watermark) if watermark else None

    if since_year:
        logger.info("Incremental run — fetching Sit@del2 years > %d", since_year)
    else:
        logger.info("Full load — fetching all Sit@del2 data")

    params = build_params(since_year)
    total_inserted = 0
    max_year_seen: int | None = None

    for records in fetch_dido_pages(endpoint, params):
        df = parse(records)
        if df.empty:
            continue

        if "annee" in df.columns:
            valid = df["annee"].dropna()
            if not valid.empty:
                year = int(valid.max())
                if max_year_seen is None or year > max_year_seen:
                    max_year_seen = year

        if dry_run:
            logger.info("[dry-run] Would insert %d rows", len(df))
        else:
            total_inserted += load_df(client, df, TABLE)

    if not dry_run and max_year_seen is not None:
        set_watermark(client, SOURCE, str(max_year_seen))

    logger.info("Done — %d rows inserted (dry_run=%s)", total_inserted, dry_run)
    return total_inserted


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Sit@del2 (autorisations urbanisme) from DiDo API."
    )
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignore the watermark and reload all data.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and parse without writing to ClickHouse.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    run(full_refresh=args.full_refresh, dry_run=args.dry_run)
