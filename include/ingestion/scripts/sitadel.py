"""
Ingestion incrémentale Sit@del2 — Autorisations et mises en chantier.

Source    : SDES / API DiDo v1
Table     : db_wh_housing.raw_sitadel
Granularité : mensuelle × commune (28,5 M lignes)
Watermark : "YYYY-MM" (dernier mois entièrement chargé)

Stratégie incrémentale :
  Les données sont triées par -ANNEE,-MOIS (plus récent en premier).
  On s'arrête dès qu'une page contient des lignes antérieures au watermark,
  ce qui évite de relire 28 M lignes à chaque run mensuel.

Env vars requis :
    CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD

RID DiDo hardcodé : 577a8a66-4157-4787-b00a-031b61afea61
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
TABLE = "db_wh_housing.raw_sitadel"
_DIDO_BASE = "https://data.statistiques.developpement-durable.gouv.fr/dido/api/v1"
_RID = "577a8a66-4157-4787-b00a-031b61afea61"

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

# DiDo column name → notre schéma
_RENAME_MAP = {
    "ANNEE":    "annee",
    "MOIS":     "mois",
    "CODE_INSEE": "code_commune",
    "TYPE_LGT": "type_logement",
    "LOG_AUT":  "nb_logements_autorises",
    "LOG_COM":  "nb_logements_commences",
    "SDP_AUT":  "surface_autorisee",
    "SDP_COM":  "surface_commencee",
}

_INT_COLS    = ["mois", "nb_logements_autorises", "nb_logements_commences"]
_FLOAT_COLS  = ["surface_autorisee", "surface_commencee"]
_STRING_COLS = ["code_commune", "code_departement", "type_logement"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dept_from_commune(code: str) -> str:
    """Derive department code from INSEE commune code."""
    if code[:2] in ("97", "98"):
        return code[:3]
    return code[:2]


def _parse_watermark(wm: str | None) -> tuple[int, int] | None:
    """Parse "YYYY-MM" watermark into (year, month) ints, or None."""
    if wm and len(wm) >= 7:
        try:
            return int(wm[:4]), int(wm[5:7])
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# Extract / Transform
# ---------------------------------------------------------------------------

def parse(records: list[dict]) -> pd.DataFrame:
    """
    Rename DiDo columns, derive code_departement, cast types.
    Returns an empty DataFrame when records is empty.
    """
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records).rename(columns=_RENAME_MAP)

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

    output_cols = list(_RENAME_MAP.values()) + ["code_departement"]
    present = [c for c in output_cols if c in df.columns]
    return df[present]


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def run(*, full_refresh: bool = False, dry_run: bool = False) -> int:
    """
    Ingest Sit@del2 data incrementally from DiDo API into ClickHouse.

    Data is fetched most-recent-first (orderBy=-ANNEE,-MOIS). Pagination
    stops as soon as a page contains rows older than the watermark.
    Returns the total number of rows inserted (0 in dry-run mode).
    """
    endpoint = f"{_DIDO_BASE}/datafiles/{_RID}/rows"

    client = get_client()
    ensure_watermark_table(client)
    client.command(DDL)

    watermark = None if full_refresh else get_watermark(client, SOURCE)
    since_ym = _parse_watermark(watermark)

    if since_ym:
        logger.info(
            "Incremental run — fetching Sit@del2 after %d-%02d", *since_ym
        )
    else:
        logger.info("Full load — fetching all Sit@del2 data (28 M rows)")

    params = {"orderBy": "-ANNEE,-MOIS"}
    total_inserted = 0
    max_ym: tuple[int, int] | None = None

    for records in fetch_dido_pages(endpoint, params):
        df = parse(records)
        if df.empty:
            continue

        has_old = False
        if since_ym and "annee" in df.columns and "mois" in df.columns:
            wm_year, wm_month = since_ym
            new_mask = (df["annee"] > wm_year) | (
                (df["annee"] == wm_year) & (df["mois"] > wm_month)
            )
            has_old = bool((~new_mask).any())
            df = df[new_mask]

        if not df.empty:
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

        if has_old:
            break

    if not dry_run and max_ym is not None:
        wm_str = f"{max_ym[0]}-{max_ym[1]:02d}"
        set_watermark(client, SOURCE, wm_str)

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
