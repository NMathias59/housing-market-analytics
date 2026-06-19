"""
Ingestion incrémentale DPE — Diagnostics de Performance Énergétique.

Source    : ADEME / data.gouv.fr (fichiers CSV.GZ annuels)
Table     : db_logement_raw.raw_dpe
Watermark : annee (dernière année chargée, dérivée de date_etablissement_dpe)

Env vars requis :
    CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
    DPE_BASE_URL   — base URL des fichiers annuels ADEME (sans année ni extension)
    DPE_TARGET_YEAR — dernière année à charger (défaut: année courante - 1)

Usage :
    python -m include.ingestion.scripts.dpe
    python -m include.ingestion.scripts.dpe --full-refresh
    python -m include.ingestion.scripts.dpe --dry-run
    python -m include.ingestion.scripts.dpe --year 2023
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime

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

SOURCE = "dpe"
TABLE = "db_logement_raw.raw_dpe"
CHUNK_SIZE = 50_000

_DEFAULT_TARGET_YEAR = datetime.now().year - 1

DDL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE}
    (
        numero_dpe                       String,
        date_etablissement_dpe           Date,
        annee                            UInt16,
        code_commune                     String,
        code_departement                 LowCardinality(String),
        etiquette_dpe                    LowCardinality(String),
        consommation_energie_primaire    Nullable(Float64),
        consommation_energie_finale      Nullable(Float64),
        emission_ges                     Nullable(Float64),
        type_energie_principale_chauffage LowCardinality(String),
        type_installation_chauffage      LowCardinality(String),
        surface_habitable_logement       Nullable(Float64),
        annee_construction               Nullable(UInt16),
        type_batiment                    LowCardinality(String),
        _loaded_at                       DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(_loaded_at)
    ORDER BY (annee, code_commune, numero_dpe)
    SETTINGS allow_nullable_key = 1
"""

_FLOAT_COLS = [
    "consommation_energie_primaire",
    "consommation_energie_finale",
    "emission_ges",
    "surface_habitable_logement",
]
_INT_COLS = ["annee_construction"]
_STRING_COLS = [
    "numero_dpe", "code_commune", "code_departement", "etiquette_dpe",
    "type_energie_principale_chauffage", "type_installation_chauffage", "type_batiment",
]
COLUMNS = _STRING_COLS + ["date_etablissement_dpe"] + _FLOAT_COLS + _INT_COLS


# ---------------------------------------------------------------------------
# Extract / Transform
# ---------------------------------------------------------------------------

def build_file_url(year: int) -> str:
    """Return the DPE CSV.GZ URL for the given year."""
    base = os.environ.get("DPE_BASE_URL", "")
    if not base:
        raise EnvironmentError(
            "DPE_BASE_URL is not set. "
            "Set it to the base URL of the ADEME annual DPE files."
        )
    return f"{base}/{year}.csv.gz"


def _target_year() -> int:
    raw = os.environ.get("DPE_TARGET_YEAR", "")
    return int(raw) if raw else _DEFAULT_TARGET_YEAR


def _years_to_load(since_year: int | None, target_year: int) -> list[int]:
    start = (since_year + 1) if since_year is not None else 2021
    return list(range(start, target_year + 1))


def parse(chunk: pd.DataFrame) -> pd.DataFrame:
    """Cast a raw DPE chunk to the target schema, deriving annee from date."""
    present = [c for c in COLUMNS if c in chunk.columns]
    df = chunk[present].copy()

    if "date_etablissement_dpe" in df.columns:
        df["date_etablissement_dpe"] = pd.to_datetime(
            df["date_etablissement_dpe"], errors="coerce"
        )
        df["annee"] = df["date_etablissement_dpe"].dt.year.astype("Int64")

    for col in _FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].str.replace(",", "."), errors="coerce"
            )

    for col in _INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in _STRING_COLS:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    return df


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def run(
    *,
    full_refresh: bool = False,
    dry_run: bool = False,
    year: int | None = None,
) -> int:
    """
    Ingest DPE data incrementally from ADEME annual files into ClickHouse.

    Returns the total number of rows inserted (0 in dry-run mode).
    Watermark updated after each year to survive mid-run crashes.
    """
    client = get_client()
    ensure_watermark_table(client)
    client.command(DDL)

    target = year if year is not None else _target_year()

    if year is not None:
        years = [year]
    else:
        watermark = None if full_refresh else get_watermark(client, SOURCE)
        since_year = int(watermark) if watermark else None
        years = _years_to_load(since_year, target)

    if not years:
        logger.info("DPE already up to date (target=%d)", target)
        return 0

    logger.info("DPE — loading years %s (full_refresh=%s, dry_run=%s)",
                years, full_refresh, dry_run)

    total_inserted = 0

    for y in years:
        url = build_file_url(y)
        logger.info("Processing DPE year %d — %s", y, url)
        year_inserted = 0

        for chunk in iter_csv_chunks(url, CHUNK_SIZE):
            df = parse(chunk)
            if df.empty:
                continue
            if dry_run:
                logger.info("[dry-run] Would insert %d rows (year %d)", len(df), y)
            else:
                year_inserted += load_df(client, df, TABLE)

        if not dry_run:
            total_inserted += year_inserted
            if year is None:
                set_watermark(client, SOURCE, str(y))
            logger.info("Year %d done — %d rows inserted", y, year_inserted)

    logger.info("Done — %d total rows inserted (dry_run=%s)", total_inserted, dry_run)
    return total_inserted


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest DPE (diagnostics énergétiques) from ADEME into ClickHouse."
    )
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignore the watermark and reload all years.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Download and parse without writing to ClickHouse.")
    parser.add_argument("--year", type=int, default=None,
                        help="Load a single specific year.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    run(full_refresh=args.full_refresh, dry_run=args.dry_run, year=args.year)
