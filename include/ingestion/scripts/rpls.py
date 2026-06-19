"""
Ingestion incrémentale RPLS — Répertoire des logements sociaux.

Source    : SDES / API DiDo v1
Table     : db_wh_housing.raw_rpls
Granularité : annuelle (snapshot par millésime), 1 ligne par logement

Stratégie incrémentale par millésime :
  DiDo publie plusieurs millésimes du RPLS (ex. 2022-01, 2023-01, 2024-01, 2025-01).
  On charge uniquement les millésimes postérieurs au watermark.
  L'année (annee) est dérivée de la date du millésime (ex. "2024-01" → 2024).

Colonnes DiDo retenues → schéma cible :
  DEPCOM     → code_commune
  DEP_CODE   → code_departement
  REG_CODE   → code_region
  FINAN_CODE → financement
  DPEENERGIE → classe_energie_dpe
  CONSTRUCT  → annee_construction
  SURFHAB    → surface
  NBPIECE    → nb_pieces
  + annee    (ajouté depuis le millésime)

RID DiDo hardcodé  : f3c2f2cb-8fb1-40fd-8733-964247744c9a
Dataset DiDo ID    : 6390f7cb84f0679b04942fc2

Env vars requis :
    CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD

Usage :
    python -m include.ingestion.scripts.rpls
    python -m include.ingestion.scripts.rpls --full-refresh
    python -m include.ingestion.scripts.rpls --dry-run
"""

from __future__ import annotations

import argparse
import logging

import pandas as pd
import requests
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

SOURCE = "rpls"
TABLE = "db_wh_housing.raw_rpls"
_DIDO_BASE = "https://data.statistiques.developpement-durable.gouv.fr/dido/api/v1"
_DATASET_ID = "6390f7cb84f0679b04942fc2"
_RID = "f3c2f2cb-8fb1-40fd-8733-964247744c9a"

DDL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE}
    (
        annee              UInt16,
        code_commune       String,
        code_departement   LowCardinality(String),
        code_region        LowCardinality(String),
        financement        LowCardinality(String),
        classe_energie_dpe LowCardinality(String),
        annee_construction Nullable(UInt16),
        surface            Nullable(Float64),
        nb_pieces          Nullable(UInt8),
        _loaded_at         DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(_loaded_at)
    ORDER BY (annee, code_commune, financement)
    SETTINGS allow_nullable_key = 1
"""

_RENAME_MAP = {
    "DEPCOM":     "code_commune",
    "DEP_CODE":   "code_departement",
    "REG_CODE":   "code_region",
    "FINAN_CODE": "financement",
    "DPEENERGIE": "classe_energie_dpe",
    "CONSTRUCT":  "annee_construction",
    "SURFHAB":    "surface",
    "NBPIECE":    "nb_pieces",
}

_INT_COLS    = ["annee_construction", "nb_pieces"]
_FLOAT_COLS  = ["surface"]
_STRING_COLS = ["code_commune", "code_departement", "code_region",
                "financement", "classe_energie_dpe"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_millesimes() -> list[str]:
    """Return available millesimes for the RPLS datafile, sorted ascending."""
    r = requests.get(f"{_DIDO_BASE}/datasets/{_DATASET_ID}", timeout=15)
    r.raise_for_status()
    data = r.json()
    for df_info in data.get("datafiles", []):
        if df_info.get("rid") == _RID:
            return sorted(m["millesime"] for m in df_info.get("millesimes", []))
    return []


# ---------------------------------------------------------------------------
# Extract / Transform
# ---------------------------------------------------------------------------

def parse(records: list[dict], annee: int) -> pd.DataFrame:
    """
    Rename DiDo columns, add annee from millesime year, cast types.
    Returns an empty DataFrame when records is empty.
    """
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records).rename(columns=_RENAME_MAP)
    df["annee"] = annee

    for col in _INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in _FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in _STRING_COLS:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    output_cols = ["annee"] + list(_RENAME_MAP.values())
    present = [c for c in output_cols if c in df.columns]
    return df[present].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def run(*, full_refresh: bool = False, dry_run: bool = False) -> int:
    """
    Ingest RPLS data incrementally by DiDo millesime into ClickHouse.

    Loads only millesimes newer than the stored watermark.
    Returns total rows inserted (0 in dry-run mode).
    """
    client = get_client()
    ensure_watermark_table(client)
    client.command(DDL)

    watermark = None if full_refresh else get_watermark(client, SOURCE)

    millesimes = get_millesimes()
    if not millesimes:
        logger.warning("RPLS: no millesimes found in DiDo catalogue")
        return 0

    to_load = [m for m in millesimes if watermark is None or m > watermark]
    if not to_load:
        logger.info("RPLS already up to date (watermark=%s)", watermark)
        return 0

    logger.info("RPLS — loading millesimes %s", to_load)

    endpoint = f"{_DIDO_BASE}/datafiles/{_RID}/rows"
    total_inserted = 0

    for millesime in to_load:
        annee = int(millesime[:4])
        logger.info("Processing RPLS millesime %s (annee=%d)", millesime, annee)
        mil_inserted = 0

        for records in fetch_dido_pages(endpoint, {"millesime": millesime}):
            df = parse(records, annee)
            if df.empty:
                continue
            if dry_run:
                logger.info("[dry-run] Would insert %d rows (millesime %s)",
                            len(df), millesime)
            else:
                mil_inserted += load_df(client, df, TABLE)

        if not dry_run:
            total_inserted += mil_inserted
            set_watermark(client, SOURCE, millesime)
            logger.info("Millesime %s done — %d rows inserted", millesime, mil_inserted)

    logger.info("Done — %d total rows inserted (dry_run=%s)", total_inserted, dry_run)
    return total_inserted


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest RPLS (logements sociaux) from DiDo API by millesime."
    )
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignore the watermark and reload all millesimes.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and parse without writing to ClickHouse.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    run(full_refresh=args.full_refresh, dry_run=args.dry_run)
