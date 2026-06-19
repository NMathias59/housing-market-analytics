"""
Ingestion EPTB — Prix des terrains et du bâti (maisons individuelles).

Source    : SDES / API DiDo v1
Table     : db_logement_raw.raw_eptb
Granularité : annuelle × région (~252 lignes par table)

Deux datafiles DiDo sont fusionnés sur (annee, zone_code) :
  - Terrains achetés  : nombre, surface et prix moyen par région
  - Maisons construites : nombre, surface et prix moyen par région

Les données étant très petites (~504 lignes total), la table est rechargée
intégralement à chaque run. Pas de watermark ni de filtrage incrémental.

RIDs DiDo hardcodés (stables, basés sur ObjectID MongoDB) :
  Terrains : 7b0b1184-f92e-4f8a-8a6a-19b4b23d5118
  Maisons  : d23a979d-8a05-4cc5-a434-c8e19ca9259c

Env vars requis :
    CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD

Usage :
    python -m include.ingestion.scripts.eptb
    python -m include.ingestion.scripts.eptb --dry-run
"""

from __future__ import annotations

import argparse
import logging

import pandas as pd
from dotenv import load_dotenv

from include.ingestion.base import (
    ensure_watermark_table,
    fetch_dido_pages,
    get_client,
    load_df,
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

SOURCE = "eptb"
TABLE = "db_logement_raw.raw_eptb"
_DIDO_BASE = "https://data.statistiques.developpement-durable.gouv.fr/dido/api/v1"

_RID_TERRAINS = "7b0b1184-f92e-4f8a-8a6a-19b4b23d5118"
_RID_MAISONS  = "d23a979d-8a05-4cc5-a434-c8e19ca9259c"

DDL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE}
    (
        annee               UInt16,
        zone_code           LowCardinality(String),
        zone_libelle        LowCardinality(String),
        nb_terrains         Nullable(Int32),
        prix_terrain_m2_moy Nullable(Float64),
        prix_terrain_m2_med Nullable(Float64),
        surface_terrain_moy Nullable(Float64),
        prix_terrain_total  Nullable(Float64),
        nb_maisons          Nullable(Int32),
        prix_maison_m2_moy  Nullable(Float64),
        prix_maison_m2_med  Nullable(Float64),
        surface_maison_moy  Nullable(Float64),
        prix_maison_total   Nullable(Float64),
        _loaded_at          DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(_loaded_at)
    ORDER BY (annee, zone_code)
    SETTINGS allow_nullable_key = 1
"""

_RENAME_TERRAINS = {
    "ANNEE":       "annee",
    "ZONE_CODE":   "zone_code",
    "ZONE_LIBELLE":"zone_libelle",
    "NB_TERRAINS": "nb_terrains",
    "PTM2_MOY":    "prix_terrain_m2_moy",
    "PTM2_MED":    "prix_terrain_m2_med",
    "SURFT_MOY":   "surface_terrain_moy",
    "PT_MOY":      "prix_terrain_total",
}

_RENAME_MAISONS = {
    "ANNEE":       "annee",
    "ZONE_CODE":   "zone_code",
    "ZONE_LIBELLE":"zone_libelle",
    "NB_MAISONS":  "nb_maisons",
    "PMM2_MOY":    "prix_maison_m2_moy",
    "PMM2_MED":    "prix_maison_m2_med",
    "SURFM_MOY":   "surface_maison_moy",
    "PM_MOY":      "prix_maison_total",
}


# ---------------------------------------------------------------------------
# Extract / Transform
# ---------------------------------------------------------------------------

def _fetch_table(rid: str, rename_map: dict) -> pd.DataFrame:
    """Fetch all pages for a DiDo RID and return a renamed DataFrame."""
    endpoint = f"{_DIDO_BASE}/datafiles/{rid}/rows"
    frames: list[pd.DataFrame] = []
    for records in fetch_dido_pages(endpoint, {}):
        if not records:
            continue
        df = pd.DataFrame(records).rename(columns=rename_map)
        target_cols = [c for c in rename_map.values() if c in df.columns]
        frames.append(df[target_cols])
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def parse(df_terrains: pd.DataFrame, df_maisons: pd.DataFrame) -> pd.DataFrame:
    """
    Merge terrain and maison tables on (annee, zone_code, zone_libelle),
    cast numeric columns.
    """
    if df_terrains.empty and df_maisons.empty:
        return pd.DataFrame()

    merged = pd.merge(
        df_terrains, df_maisons,
        on=["annee", "zone_code", "zone_libelle"],
        how="outer",
    )

    if "annee" in merged.columns:
        merged["annee"] = pd.to_numeric(merged["annee"], errors="coerce").astype("Int64")

    int_cols   = ["nb_terrains", "nb_maisons"]
    float_cols = [
        "prix_terrain_m2_moy", "prix_terrain_m2_med", "surface_terrain_moy",
        "prix_terrain_total", "prix_maison_m2_moy", "prix_maison_m2_med",
        "surface_maison_moy", "prix_maison_total",
    ]
    str_cols = ["zone_code", "zone_libelle"]

    for col in int_cols:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce").astype("Int64")

    for col in float_cols:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")

    for col in str_cols:
        if col in merged.columns:
            merged[col] = merged[col].fillna("").astype(str)

    return merged.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def run(*, dry_run: bool = False) -> int:
    """
    Load EPTB terrains + maisons from DiDo, merge and insert into ClickHouse.

    Full reload every run (~504 rows). Returns rows inserted (0 in dry-run).
    """
    client = get_client()
    ensure_watermark_table(client)
    client.command(DDL)

    logger.info("Fetching EPTB terrains (rid=%s)", _RID_TERRAINS)
    df_t = _fetch_table(_RID_TERRAINS, _RENAME_TERRAINS)

    logger.info("Fetching EPTB maisons (rid=%s)", _RID_MAISONS)
    df_m = _fetch_table(_RID_MAISONS, _RENAME_MAISONS)

    df = parse(df_t, df_m)

    if df.empty:
        logger.warning("EPTB: no data returned from DiDo")
        return 0

    logger.info("EPTB merged: %d rows, years %s–%s",
                len(df), df["annee"].min(), df["annee"].max())

    if dry_run:
        logger.info("[dry-run] Would insert %d rows", len(df))
        return 0

    inserted = load_df(client, df, TABLE)
    logger.info("Done — %d rows inserted", inserted)
    return inserted


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest EPTB (prix terrains à bâtir) from DiDo API into ClickHouse."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and parse without writing to ClickHouse.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    run(dry_run=args.dry_run)
