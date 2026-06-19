"""
Ingestion incrémentale ECLN — Commercialisation de logements neufs.

Source    : SDES / API DiDo v1
Table     : db_wh_logement_raw.raw_ecln
Granularité : trimestrielle × département (24 k lignes)

Stratégie incrémentale :
  Le dataset est petit (24 k lignes). On télécharge tout et on filtre
  côté client : seules les lignes dont TRIMESTRE > watermark sont insérées.
  L'API DiDo ne supporte pas de paramètre de filtre sur les lignes.

Watermark : "YYYY-TQ" (ex. "2024-T4" = dernier trimestre chargé)

Colonnes DiDo → schéma cible :
  TRIMESTRE    → annee (UInt16) + trimestre (UInt8)
  DEP_CODE     → code_departement
  TYPE_LGT     → type_logement
  MEV          → nb_mises_en_vente
  RESA         → nb_reservations
  ANNUL        → nb_annulations
  STOCK        → stock
  DELAI_ECOUL  → delai_ecoulement
  PRIX_M2      → prix_m2
  PRIX_MOY_IND → prix_moyen_individuel

RID DiDo hardcodé : 95e4190c-4d70-403f-9537-5b71fd005b1c
  (Ventes aux particuliers — Données trimestrielles brutes, niveau département)

Env vars requis :
    CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD

Usage :
    python -m include.ingestion.scripts.ecln
    python -m include.ingestion.scripts.ecln --full-refresh
    python -m include.ingestion.scripts.ecln --dry-run
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

SOURCE = "ecln"
TABLE = "db_wh_logement_raw.raw_ecln"
_DIDO_BASE = "https://data.statistiques.developpement-durable.gouv.fr/dido/api/v1"
_RID = "95e4190c-4d70-403f-9537-5b71fd005b1c"

DDL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE}
    (
        annee                  UInt16,
        trimestre              UInt8,
        code_departement       LowCardinality(String),
        libelle_departement    String,
        type_logement          LowCardinality(String),
        nb_mises_en_vente      Nullable(Int32),
        nb_reservations        Nullable(Int32),
        nb_annulations         Nullable(Int32),
        stock                  Nullable(Int32),
        delai_ecoulement       Nullable(Float64),
        prix_m2                Nullable(Float64),
        prix_moyen_individuel  Nullable(Float64),
        _loaded_at             DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(_loaded_at)
    ORDER BY (annee, trimestre, code_departement, type_logement)
    SETTINGS allow_nullable_key = 1
"""

_RENAME_MAP = {
    "DEP_CODE":     "code_departement",
    "DEP_LIBELLE":  "libelle_departement",
    "TYPE_LGT":     "type_logement",
    "MEV":          "nb_mises_en_vente",
    "RESA":         "nb_reservations",
    "ANNUL":        "nb_annulations",
    "STOCK":        "stock",
    "DELAI_ECOUL":  "delai_ecoulement",
    "PRIX_M2":      "prix_m2",
    "PRIX_MOY_IND": "prix_moyen_individuel",
}

_INT_COLS    = ["nb_mises_en_vente", "nb_reservations", "nb_annulations", "stock"]
_FLOAT_COLS  = ["delai_ecoulement", "prix_m2", "prix_moyen_individuel"]
_STRING_COLS = ["code_departement", "libelle_departement", "type_logement"]


# ---------------------------------------------------------------------------
# Extract / Transform
# ---------------------------------------------------------------------------

def _parse_trimestre(raw: str) -> tuple[int, int] | tuple[None, None]:
    """Parse "2024-T3" → (2024, 3). Returns (None, None) on invalid input."""
    try:
        year_str, q_str = raw.split("-T")
        return int(year_str), int(q_str)
    except (ValueError, AttributeError):
        return None, None


def parse(records: list[dict], since_trimestre: str | None = None) -> pd.DataFrame:
    """
    Rename DiDo columns, split TRIMESTRE into annee + trimestre,
    and filter rows newer than *since_trimestre* (format "YYYY-TQ").

    String comparison on "YYYY-TQ" is chronologically correct.
    """
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Derive annee and trimestre from TRIMESTRE field before renaming
    if "TRIMESTRE" in df.columns:
        df[["annee", "trimestre"]] = pd.DataFrame(
            df["TRIMESTRE"].map(_parse_trimestre).tolist(),
            index=df.index,
        )
        if since_trimestre:
            df = df[df["TRIMESTRE"] > since_trimestre]
        df = df.drop(columns=["TRIMESTRE"])

    df = df.rename(columns=_RENAME_MAP)

    if "annee" in df.columns:
        df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("Int64")
    if "trimestre" in df.columns:
        df["trimestre"] = pd.to_numeric(df["trimestre"], errors="coerce").astype("Int64")

    for col in _INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in _FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in _STRING_COLS:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    output_cols = ["annee", "trimestre"] + list(_RENAME_MAP.values())
    present = [c for c in output_cols if c in df.columns]
    return df[present].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def run(*, full_refresh: bool = False, dry_run: bool = False) -> int:
    """
    Ingest ECLN data from DiDo API into ClickHouse.

    Downloads the full dataset each run (24 k rows) and filters
    client-side. Returns total rows inserted (0 in dry-run mode).
    """
    endpoint = f"{_DIDO_BASE}/datafiles/{_RID}/rows"

    client = get_client()
    ensure_watermark_table(client)
    client.command(DDL)

    watermark = None if full_refresh else get_watermark(client, SOURCE)

    if watermark:
        logger.info("Incremental run — fetching ECLN after trimestre %s", watermark)
    else:
        logger.info("Full load — fetching all ECLN data")

    total_inserted = 0
    max_trimestre: str | None = None

    for records in fetch_dido_pages(endpoint, {}):
        df = parse(records, since_trimestre=watermark)
        if df.empty:
            continue

        # Track max TRIMESTRE seen (re-derive from annee + trimestre)
        if "annee" in df.columns and "trimestre" in df.columns:
            for _, row in df[["annee", "trimestre"]].dropna().iterrows():
                candidate = f"{int(row['annee'])}-T{int(row['trimestre'])}"
                if max_trimestre is None or candidate > max_trimestre:
                    max_trimestre = candidate

        if dry_run:
            logger.info("[dry-run] Would insert %d rows", len(df))
        else:
            total_inserted += load_df(client, df, TABLE)

    if not dry_run and max_trimestre is not None:
        set_watermark(client, SOURCE, max_trimestre)

    logger.info("Done — %d rows inserted (dry_run=%s)", total_inserted, dry_run)
    return total_inserted


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest ECLN (commercialisation logements neufs) from DiDo API."
    )
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignore the watermark and reload all data.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and parse without writing to ClickHouse.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    run(full_refresh=args.full_refresh, dry_run=args.dry_run)
