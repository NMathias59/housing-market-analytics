"""
Ingestion incrémentale DVF — Demandes de Valeurs Foncières.

Source    : DGFiP / data.gouv.fr (geo-dvf, fichiers CSV.GZ annuels)
Table     : db_wh_housing.raw_dvf
Watermark : annee (dernière année entièrement chargée)

Les fichiers DVF sont volumineux (>1 Go/an décompressé). Le script :
  1. Identifie les années à charger (watermark+1 → année cible)
  2. Télécharge chaque fichier en streaming gzip — pas d'écriture disque
  3. Charge par chunks de CHUNK_SIZE lignes dans ClickHouse

Env vars requis :
    CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
    DVF_BASE_URL   — base URL des fichiers (défaut: geo-dvf data.gouv.fr)
    DVF_TARGET_YEAR — dernière année à charger (défaut: année courante - 1)

Usage :
    python -m include.ingestion.scripts.dvf
    python -m include.ingestion.scripts.dvf --full-refresh
    python -m include.ingestion.scripts.dvf --dry-run
    python -m include.ingestion.scripts.dvf --year 2023
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime
from typing import Iterator

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

SOURCE = "dvf"
TABLE = "db_wh_housing.raw_dvf"
CHUNK_SIZE = 50_000

_DVF_BASE_URL = os.environ.get(
    "DVF_BASE_URL",
    "https://files.data.gouv.fr/geo-dvf/latest/csv",
)
_DEFAULT_TARGET_YEAR = datetime.now().year - 1

DDL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE}
    (
        id_mutation               String,
        date_mutation             Date,
        annee                     UInt16,
        nature_mutation           LowCardinality(String),
        valeur_fonciere           Nullable(Float64),
        adresse_numero            Nullable(String),
        adresse_nom_voie          String,
        code_postal               LowCardinality(String),
        code_commune              String,
        nom_commune               String,
        code_departement          LowCardinality(String),
        id_parcelle               String,
        type_local                LowCardinality(String),
        surface_reelle_bati       Nullable(Float64),
        nombre_pieces_principales Nullable(Int16),
        surface_terrain           Nullable(Float64),
        latitude                  Nullable(Float64),
        longitude                 Nullable(Float64),
        _loaded_at                DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(_loaded_at)
    ORDER BY (annee, code_commune, id_mutation)
    SETTINGS allow_nullable_key = 1
"""

_FLOAT_COLS = ["valeur_fonciere", "surface_reelle_bati", "surface_terrain",
               "latitude", "longitude"]
_INT_COLS = ["nombre_pieces_principales"]
_DATE_COLS = ["date_mutation"]
_STRING_COLS = ["id_mutation", "nature_mutation", "adresse_numero", "adresse_nom_voie",
                "code_postal", "code_commune", "nom_commune", "code_departement",
                "id_parcelle", "type_local"]
COLUMNS = _STRING_COLS + _DATE_COLS + _FLOAT_COLS + _INT_COLS


# ---------------------------------------------------------------------------
# Extract / Transform
# ---------------------------------------------------------------------------

def build_file_url(year: int) -> str:
    """Return the geo-dvf CSV.GZ URL for the given year."""
    base = os.environ.get("DVF_BASE_URL", _DVF_BASE_URL)
    return f"{base}/{year}/full.csv.gz"


def _target_year() -> int:
    raw = os.environ.get("DVF_TARGET_YEAR", "")
    return int(raw) if raw else _DEFAULT_TARGET_YEAR



def parse(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Cast a raw DVF chunk to the target schema.

    Derives *annee* from *date_mutation*. Coerces invalid values to NaN/NaT
    rather than raising — data quality is enforced downstream by dbt tests.
    """
    present = [c for c in COLUMNS if c in chunk.columns]
    df = chunk[present].copy()

    if "date_mutation" in df.columns:
        df["date_mutation"] = pd.to_datetime(df["date_mutation"], errors="coerce")
        df["annee"] = df["date_mutation"].dt.year.astype("Int64")

    for col in _FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(
            df[col].str.replace("\xa0", "").str.replace(" ", "").str.replace(",", "."),
            errors="coerce",
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

def _years_to_load(since_year: int | None, target_year: int) -> list[int]:
    """Return the list of years to fetch, from watermark+1 to target_year."""
    start = (since_year + 1) if since_year is not None else 2014
    return list(range(start, target_year + 1))


def run(
    *,
    full_refresh: bool = False,
    dry_run: bool = False,
    year: int | None = None,
) -> int:
    """
    Ingest DVF data incrementally from geo-dvf into ClickHouse.

    If *year* is set, only that year is loaded (overrides watermark logic).
    Returns the total number of rows inserted (0 in dry-run mode).
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
        logger.info("DVF already up to date (watermark=%s, target=%d)", watermark, target)
        return 0

    logger.info(
        "DVF — loading years %s (full_refresh=%s, dry_run=%s)",
        years, full_refresh, dry_run,
    )

    total_inserted = 0

    for y in years:
        url = build_file_url(y)
        logger.info("Processing DVF year %d — %s", y, url)
        year_inserted = 0

        for chunk in iter_csv_chunks(url):
            df = parse(chunk)
            if df.empty:
                continue
            if dry_run:
                logger.info("[dry-run] Would insert %d rows (year %d)", len(df), y)
            else:
                year_inserted += load_df(client, df, TABLE)

        if not dry_run:
            total_inserted += year_inserted
            # Watermark updated per year so a crash mid-run doesn't lose progress
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
        description="Ingest DVF (transactions immobilières) from geo-dvf into ClickHouse."
    )
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignore the watermark and reload all years.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Download and parse without writing to ClickHouse.")
    parser.add_argument("--year", type=int, default=None,
                        help="Load a single specific year (overrides watermark).")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    run(full_refresh=args.full_refresh, dry_run=args.dry_run, year=args.year)
