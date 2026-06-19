"""
DAG d'ingestion dynamique — marché immobilier français.

Toutes les sources sont ingérées en parallèle dans db_wh_housing.
La cadence est mensuelle ; les sources trimestrielles/annuelles
retournent immédiatement "déjà à jour" grâce aux watermarks.

Sources :
    dvf      — Demandes de Valeurs Foncières (DGFiP)
    dpe      — Diagnostics de Performance Énergétique (ADEME)
    sitadel  — Permis de construire Sit@del2 (SDES/DiDo)
    ecln     — Commercialisation logements neufs (SDES/DiDo)
    eptb     — Prix terrains et maisons neuves (SDES/DiDo)
    rpls     — Répertoire des logements sociaux (SDES/DiDo)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Config des sources — ajouter une ligne pour brancher une nouvelle source
# ---------------------------------------------------------------------------

SOURCES: list[dict] = [
    {
        "source_id": "dvf",
        "module":    "include.ingestion.scripts.dvf",
        "doc":       "DVF — Demandes de Valeurs Foncières (DGFiP)",
    },
    {
        "source_id": "dpe",
        "module":    "include.ingestion.scripts.dpe",
        "doc":       "DPE — Diagnostics de Performance Énergétique (ADEME)",
    },
    {
        "source_id": "sitadel",
        "module":    "include.ingestion.scripts.sitadel",
        "doc":       "Sit@del2 — Permis de construire (SDES/DiDo)",
    },
    {
        "source_id": "ecln",
        "module":    "include.ingestion.scripts.ecln",
        "doc":       "ECLN — Commercialisation logements neufs (SDES/DiDo)",
    },
    {
        "source_id": "eptb",
        "module":    "include.ingestion.scripts.eptb",
        "doc":       "EPTB — Prix terrains et maisons neuves (SDES/DiDo)",
    },
    {
        "source_id": "rpls",
        "module":    "include.ingestion.scripts.rpls",
        "doc":       "RPLS — Répertoire des logements sociaux (SDES/DiDo)",
    },
]

# ---------------------------------------------------------------------------
# Factory — évite la capture de variable en boucle
# ---------------------------------------------------------------------------

def _make_callable(module_path: str):
    def _run(**ctx):
        import importlib
        mod = importlib.import_module(module_path)
        mod.run()
    return _run


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="ingestion_housing",
    description="Ingestion parallèle de toutes les sources immobilières → db_wh_housing",
    schedule="0 4 1 * *",   # 1er du mois à 04h00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "email_on_failure": False,
    },
    tags=["ingestion", "housing"],
) as dag:

    for source in SOURCES:
        PythonOperator(
            task_id=f"ingest_{source['source_id']}",
            python_callable=_make_callable(source["module"]),
            doc_md=source["doc"],
        )

    # Toutes les tasks sont au même niveau → exécution en parallèle par défaut
