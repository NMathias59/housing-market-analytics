"""
Ingestion incrémentale Sit@del2 — Autorisations et mises en chantier.

Source    : SDES / API DiDo v1
Table     : db_wh_housing.raw_sitadel
Granularité : mensuelle × commune (28,5 M lignes)
Watermark : "YYYY-MM" (dernier mois entièrement chargé)

Stratégie incrémentale :
  DiDo publie un seul millesime contenant l'intégralité des données (2013→).
  On télécharge le CSV en streaming et on filtre côté client les lignes
  postérieures au watermark — beaucoup plus rapide que la pagination JSON
  (1 download vs 285 000 appels API à 100 lignes/page).

Env vars requis :
    CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD

RID DiDo     : 577a8a66-4157-4787-b00a-031b61afea61
Dataset DiDo : 660432ce0b8987ef5dd9465d
  (Logements autorisés et commencés, séries mensuelles communales)

Usage :
    python -m include.ingestion.scripts.sitadel
    python -m include.ingestion.scripts.sitadel --full-refresh
    python -m include.ingestion.scripts.sitadel --dry-run
"""

from __future__ import annotations

import argparse
import logging

import pandas as pd
from dotenv import load_dotenv

from include.ingestion.base import (
    ensure_watermark_table,
    get_client,
    get_json,
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

SOURCE = "sitadel"
TABLE = "db_wh_housing.raw_sitadel"
_DIDO_BASE = "https://data.statistiques.developpement-durable.gouv.fr/dido/api/v1"
_RID = "577a8a66-4157-4787-b00a-031b61afea61"
_DATASET_ID = "660432ce0b8987ef5dd9465d"

DDL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE}
    (
        annee                      UInt16,
        mois                       UInt8,
        code_commune               String,
        code_departement           LowCardinality(String),
        type_logement              LowCardinality(String),
        nb_logements_autorises     Nullable(Int32),
        nb_logements_commences     Nullable(Int32),
        surface_autorisee          Nullable(Float64),
        surface_commencee          Nullable(Float64),
        _loaded_at                 DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(_loaded_at)
    ORDER BY (annee, mois, code_commune, type_logement)
    SETTINGS allow_nullable_key = 1
"""

_RENAME_MAP = {
    "ANNEE":      "annee",
    "MOIS":       "mois",
    "CODE_INSEE": "code_commune",
    "TYPE_LGT":   "type_logement",
    "LOG_AUT":    "nb_logements_autorises",
    "LOG_COM":    "nb_logements_commences",
    "SDP_AUT":    "surface_autorisee",
    "SDP_COM":    "surface_commencee",
}

_INT_COLS    = ["mois", "nb_logements_autorises", "nb_logements_commences"]
_FLOAT_COLS  = ["surface_autorisee", "surface_commencee"]
_STRING_COLS = ["code_commune", "code_departement", "type_logement"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_latest_millesime() -> str:
    """Return the most recent millesime available for the Sitadel datafile."""
    data = get_json(f"{_DIDO_BASE}/datasets/{_DATASET_ID}")
    for df_info in data.get("datafiles", []):
        if df_info.get("rid") == _RID:
            millesimes = sorted(m["millesime"] for m in df_info.get("millesimes", []))
            if millesimes:
                return millesimes[-1]
    raise RuntimeError("No millesime available for Sitadel in DiDo catalogue — will retry tomorrow")


def _dept_from_commune(code: str) -> str:
    if code[:2] in ("97", "98"):
        return code[:3]
    return code[:2]


def _parse_watermark(wm: str | None) -> tuple[int, int] | None:
    if wm and len(wm) >= 7:
        try:
            return int(wm[:4]), int(wm[5:7])
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# Extract / Transform
# ---------------------------------------------------------------------------

def parse(chunk: pd.DataFrame, since_ym: tuple[int, int] | None) -> pd.DataFrame:
    """
    Rename CSV columns, derive code_departement, cast types,
    and filter rows strictly newer than since_ym.
    """
    if chunk.empty:
        return pd.DataFrame()

    df = chunk.rename(columns=_RENAME_MAP)

    if "code_commune" in df.columns:
        df["code_commune"] = df["code_commune"].fillna("").astype(str)
        df["code_departement"] = df["code_commune"].map(_dept_from_commune)

    if "annee" in df.columns:
        df["annee"] = pd.to_numeric(df["annee"], errors="coerce").astype("Int64")

    for col in _INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in _FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in _STRING_COLS:
        if col in df.columns and col != "code_commune":
            df[col] = df[col].fillna("").astype(str)

    if since_ym and "annee" in df.columns and "mois" in df.columns:
        wm_year, wm_month = since_ym
        new_mask = (df["annee"] > wm_year) | (
            (df["annee"] == wm_year) & (df["mois"] > wm_month)
        )
        df = df[new_mask]

    output_cols = list(_RENAME_MAP.values()) + ["code_departement"]
    present = [c for c in output_cols if c in df.columns]
    return df[present].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def run(*, full_refresh: bool = False, dry_run: bool = False) -> int:
    """
    Ingest Sit@del2 data incrementally from DiDo CSV into ClickHouse.

    Streams the full millesime CSV in 50k-row chunks and filters client-side.
    Returns the total number of rows inserted (0 in dry-run mode).
    """
    client = get_client()
    ensure_watermark_table(client)
    client.command(DDL)

    watermark = None if full_refresh else get_watermark(client, SOURCE)
    since_ym = _parse_watermark(watermark)

    millesime = get_latest_millesime()

    last_millesime = get_watermark(client, f"{SOURCE}_millesime")
    if last_millesime and last_millesime == millesime:
        logger.info("Sit@del2 already up to date (millesime %s) — nothing to do", millesime)
        return 0

    csv_url = (
        f"{_DIDO_BASE}/datafiles/{_RID}/csv"
        f"?millesime={millesime}&withColumnName=true&withColumnDescription=false&withColumnUnit=false"
    )

    if since_ym:
        logger.info(
            "Incremental run — Sit@del2 millesime %s, filtering after %d-%02d",
            millesime, *since_ym,
        )
    else:
        logger.info("Full load — Sit@del2 millesime %s (28 M rows)", millesime)

    total_inserted = 0
    max_ym: tuple[int, int] | None = None

    for chunk in iter_csv_chunks(csv_url, chunk_size=50_000, sep=";"):
        df = parse(chunk, since_ym)
        if df.empty:
            continue

        if "annee" in df.columns and "mois" in df.columns:
            for yr in df["annee"].dropna().unique():
                yr_int = int(yr)
                m = int(df[df["annee"] == yr]["mois"].dropna().max())
                if max_ym is None or (yr_int, m) > max_ym:
                    max_ym = (yr_int, m)

        if dry_run:
            logger.info("[dry-run] Would insert %d rows", len(df))
        else:
            total_inserted += load_df(client, df, TABLE)

    if not dry_run and max_ym is not None:
        wm_str = f"{max_ym[0]}-{max_ym[1]:02d}"
        set_watermark(client, SOURCE, wm_str)
        set_watermark(client, f"{SOURCE}_millesime", millesime)

    logger.info("Done — %d rows inserted (dry_run=%s)", total_inserted, dry_run)
    return total_inserted


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Sit@del2 (autorisations urbanisme) from DiDo CSV."
    )
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignore the watermark and reload all data.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and parse without writing to ClickHouse.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    run(full_refresh=args.full_refresh, dry_run=args.dry_run)
