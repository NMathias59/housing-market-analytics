"""
Ingestion incrémentale DPE — Diagnostics de Performance Énergétique.

Source    : ADEME / data-fair REST API
           GET https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines
Table     : db_wh_housing.raw_dpe
Watermark : date ISO YYYY-MM-DD (dernière date_etablissement_dpe chargée)

Stratégie incrémentale :
  Filtre server-side via param qs (Lucene range) sur date_etablissement_dpe,
  tri croissant pour avancer le watermark de façon monotone. Watermark mis
  à jour après chaque page pour survivre à un crash en cours de run.

Colonnes API → schéma cible :
  numero_dpe                           → numero_dpe
  date_etablissement_dpe               → date_etablissement_dpe
  code_insee_ban                       → code_commune
  etiquette_dpe                        → etiquette_dpe
  conso_5_usages_par_m2_ep             → consommation_energie_primaire
  conso_5_usages_finale_energie_ar     → consommation_energie_finale
  emission_ges_5_usages_par_m2         → emission_ges
  type_energie_principale_chauffage    → type_energie_principale_chauffage
  type_installation_chauffage          → type_installation_chauffage
  surface_habitable_logement           → surface_habitable_logement
  periode_construction                 → periode_construction (String)
  type_batiment                        → type_batiment
  (code_departement dérivé depuis code_commune)

Env vars requis :
    CLICKHOUSE_HOST, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD

Usage :
    python -m include.ingestion.scripts.dpe
    python -m include.ingestion.scripts.dpe --full-refresh
    python -m include.ingestion.scripts.dpe --dry-run
    python -m include.ingestion.scripts.dpe --since 2023-01-01
"""

from __future__ import annotations

import argparse
import logging

import pandas as pd
from dotenv import load_dotenv

from include.ingestion.base import (
    ensure_watermark_table,
    fetch_ademe_pages,
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

SOURCE = "dpe"
TABLE = "db_wh_housing.raw_dpe"

_API_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines"
_PAGE_SIZE = 10_000
_SINCE_DATE_DEFAULT = "2021-01-01"

DDL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE}
    (
        numero_dpe                         String,
        date_etablissement_dpe             Date,
        annee                              UInt16,
        code_commune                       String,
        code_departement                   LowCardinality(String),
        etiquette_dpe                      LowCardinality(String),
        consommation_energie_primaire      Nullable(Float64),
        consommation_energie_finale        Nullable(Float64),
        emission_ges                       Nullable(Float64),
        type_energie_principale_chauffage  LowCardinality(String),
        type_installation_chauffage        LowCardinality(String),
        surface_habitable_logement         Nullable(Float64),
        periode_construction               LowCardinality(String),
        type_batiment                      LowCardinality(String),
        _loaded_at                         DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(_loaded_at)
    ORDER BY (annee, code_commune, numero_dpe)
    SETTINGS allow_nullable_key = 1
"""

_RENAME_MAP = {
    "numero_dpe":                       "numero_dpe",
    "date_etablissement_dpe":           "date_etablissement_dpe",
    "code_insee_ban":                   "code_commune",
    "etiquette_dpe":                    "etiquette_dpe",
    "conso_5_usages_par_m2_ep":         "consommation_energie_primaire",
    "conso_5_usages_finale_energie_ar": "consommation_energie_finale",
    "emission_ges_5_usages_par_m2":     "emission_ges",
    "type_energie_principale_chauffage":"type_energie_principale_chauffage",
    "type_installation_chauffage":       "type_installation_chauffage",
    "surface_habitable_logement":        "surface_habitable_logement",
    "periode_construction":              "periode_construction",
    "type_batiment":                     "type_batiment",
}

_API_FIELDS = list(_RENAME_MAP.keys())

_FLOAT_COLS = [
    "consommation_energie_primaire",
    "consommation_energie_finale",
    "emission_ges",
    "surface_habitable_logement",
]
_STRING_COLS = [
    "numero_dpe", "code_commune", "code_departement", "etiquette_dpe",
    "type_energie_principale_chauffage", "type_installation_chauffage",
    "periode_construction", "type_batiment",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dept_from_commune(code: str) -> str:
    if code.startswith(("97", "98")):
        return code[:3]
    return code[:2]


def _since_date(watermark: str | None) -> str:
    """Return the date to start loading from (day after watermark, or default)."""
    if watermark is None:
        return _SINCE_DATE_DEFAULT
    from datetime import date, timedelta
    return (date.fromisoformat(watermark) + timedelta(days=1)).isoformat()


# ---------------------------------------------------------------------------
# Extract / Transform
# ---------------------------------------------------------------------------

def parse(records: list[dict]) -> pd.DataFrame:
    """
    Rename ADEME API fields to target schema, derive annee and code_departement.
    Returns an empty DataFrame when records is empty.
    """
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records).rename(columns=_RENAME_MAP)

    if "date_etablissement_dpe" in df.columns:
        df["date_etablissement_dpe"] = pd.to_datetime(
            df["date_etablissement_dpe"], errors="coerce"
        )
        df["annee"] = df["date_etablissement_dpe"].dt.year.astype("Int64")

    if "code_commune" in df.columns:
        df["code_commune"] = df["code_commune"].fillna("").astype(str)
        df["code_departement"] = df["code_commune"].apply(
            lambda c: _dept_from_commune(c) if c else ""
        )

    for col in _FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in _STRING_COLS:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    output_cols = (
        ["numero_dpe", "date_etablissement_dpe", "annee", "code_commune",
         "code_departement"]
        + [v for v in _RENAME_MAP.values()
           if v not in {"numero_dpe", "date_etablissement_dpe",
                        "code_commune"}]
    )
    present = [c for c in output_cols if c in df.columns]
    return df[present].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def run(
    *,
    full_refresh: bool = False,
    dry_run: bool = False,
    since: str | None = None,
) -> int:
    """
    Ingest DPE data from ADEME REST API into ClickHouse, incrementally by date.

    Watermark updated after each page so a crash does not lose progress.
    Returns the total number of rows inserted (0 in dry-run mode).
    """
    client = get_client()
    ensure_watermark_table(client)
    client.command(DDL)

    if full_refresh:
        watermark = None
    elif since is not None:
        watermark = since
    else:
        watermark = get_watermark(client, SOURCE)

    params: dict = {
        "select": ",".join(_API_FIELDS),
        "sort": "date_etablissement_dpe:asc",
    }
    if not full_refresh:
        start = _since_date(watermark)
        params["qs"] = f"date_etablissement_dpe:[{start} TO *]"

    logger.info(
        "DPE — full_refresh=%s dry_run=%s watermark=%s",
        full_refresh, dry_run, watermark,
    )

    total_inserted = 0

    for records in fetch_ademe_pages(_API_URL, params, page_size=_PAGE_SIZE):
        df = parse(records)
        if df.empty:
            continue

        if dry_run:
            logger.info("[dry-run] Would insert %d DPE rows", len(df))
            continue

        total_inserted += load_df(client, df, TABLE)

        max_date = df["date_etablissement_dpe"].dropna().max()
        if not pd.isna(max_date):
            wm = str(max_date.date()) if hasattr(max_date, "date") else str(max_date)
            set_watermark(client, SOURCE, wm)

    logger.info("Done — %d rows inserted (dry_run=%s)", total_inserted, dry_run)
    return total_inserted


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest DPE (diagnostics énergétiques) from ADEME REST API."
    )
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignore the watermark and reload from the beginning.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and parse without writing to ClickHouse.")
    parser.add_argument("--since", default=None, metavar="DATE",
                        help="Override start date (YYYY-MM-DD), ignores watermark.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    run(full_refresh=args.full_refresh, dry_run=args.dry_run, since=args.since)
