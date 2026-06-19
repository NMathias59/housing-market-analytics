"""
Ingestion incrémentale INSEE — Revenus et démographie par commune.

Source    : INSEE / fichiers CSV annuels millésimés
Table     : db_logement_raw.raw_insee_communes
Watermark : annee (dernière année chargée)

Env vars requis :
    CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
    INSEE_BASE_URL — base URL des fichiers annuels (sans année ni extension)
    INSEE_TARGET_YEAR — dernière année à charger (défaut: année courante - 2,
                        les données INSEE ont un délai de publication de ~18 mois)

Usage :
    python -m include.ingestion.scripts.insee
    python -m include.ingestion.scripts.insee --full-refresh
    python -m include.ingestion.scripts.insee --dry-run
    python -m include.ingestion.scripts.insee --year 2021
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

SOURCE = "insee_communes"
TABLE = "db_logement_raw.raw_insee_communes"
CHUNK_SIZE = 10_000

# Données INSEE disponibles avec ~18 mois de décalage
_DEFAULT_TARGET_YEAR = datetime.now().year - 2

DDL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE}
    (
        code_commune              String,
        libelle_commune           String,
        annee                     UInt16,
        revenu_disponible_median  Nullable(Float64),
        taux_pauvrete             Nullable(Float64),
        population_municipale     Nullable(Int32),
        superficie                Nullable(Float64),
        densite_population        Nullable(Float64),
        part_proprietaires        Nullable(Float64),
        solde_migratoire_net      Nullable(Int32),
        part_logements_vacants    Nullable(Float64),
        _loaded_at                DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(_loaded_at)
    ORDER BY (annee, code_commune)
    SETTINGS allow_nullable_key = 1
"""

_FLOAT_COLS = [
    "revenu_disponible_median", "taux_pauvrete", "superficie",
    "densite_population", "part_proprietaires", "part_logements_vacants",
]
_INT_COLS = ["population_municipale", "solde_migratoire_net"]
_STRING_COLS = ["code_commune", "libelle_commune"]
COLUMNS = _STRING_COLS + ["annee"] + _FLOAT_COLS + _INT_COLS


# ---------------------------------------------------------------------------
# Extract / Transform
# ---------------------------------------------------------------------------

def build_file_url(year: int) -> str:
    """Return the INSEE CSV URL for the given year."""
    base = os.environ.get("INSEE_BASE_URL", "")
    if not base:
        raise EnvironmentError(
            "INSEE_BASE_URL is not set. "
            "Set it to the base URL of the INSEE annual commune files."
        )
    return f"{base}/{year}.csv"


def _target_year() -> int:
    raw = os.environ.get("INSEE_TARGET_YEAR", "")
    return int(raw) if raw else _DEFAULT_TARGET_YEAR


def _years_to_load(since_year: int | None, target_year: int) -> list[int]:
    start = (since_year + 1) if since_year is not None else 2017
    return list(range(start, target_year + 1))


def parse(chunk: pd.DataFrame) -> pd.DataFrame:
    """Cast a raw INSEE chunk to the raw_insee_communes schema."""
    present = [c for c in COLUMNS if c in chunk.columns]
    df = chunk[present].copy()

    if "annee" in df.columns:
        df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("Int64")

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
    Ingest INSEE commune data incrementally into ClickHouse.

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
        logger.info("INSEE already up to date (target=%d)", target)
        return 0

    logger.info("INSEE — loading years %s (full_refresh=%s, dry_run=%s)",
                years, full_refresh, dry_run)

    total_inserted = 0

    for y in years:
        url = build_file_url(y)
        logger.info("Processing INSEE year %d — %s", y, url)
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
        description="Ingest INSEE communes (revenus, démographie) into ClickHouse."
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
