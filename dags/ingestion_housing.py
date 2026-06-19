"""
DAGs d'ingestion du marché immobilier français.

Un DAG par source, chacun avec sa cadence naturelle :
  - DVF      : annuel  (données publiées par DGFiP début d'année N+1)
  - DPE      : mensuel (flux continu ADEME)
  - SITADEL  : mensuel (permis de construire SDES, lag ~2 mois)
  - ECLN     : trimestriel (commercialisation logements neufs)
  - EPTB     : annuel  (prix des terrains et maisons neuves)
  - RPLS     : annuel  (logements sociaux, millésime ~juillet)
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

_DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": __import__("datetime").timedelta(minutes=10),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# DVF — annuel, données N-1 publiées début février
# ---------------------------------------------------------------------------

def _run_dvf(**ctx):
    from include.ingestion.scripts.dvf import run
    run()


with DAG(
    dag_id="ingestion_dvf",
    description="DVF — Demandes de Valeurs Foncières (annuel)",
    schedule="0 6 1 2 *",  # 1er février à 06h00
    start_date=datetime(2024, 2, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["ingestion", "housing"],
) as dag_dvf:
    PythonOperator(task_id="ingest_dvf", python_callable=_run_dvf)


# ---------------------------------------------------------------------------
# DPE — mensuel (flux continu ADEME, ingestion incrémentale par date)
# ---------------------------------------------------------------------------

def _run_dpe(**ctx):
    from include.ingestion.scripts.dpe import run
    run()


with DAG(
    dag_id="ingestion_dpe",
    description="DPE — Diagnostics de Performance Énergétique (mensuel)",
    schedule="0 4 1 * *",  # 1er du mois à 04h00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["ingestion", "housing"],
) as dag_dpe:
    PythonOperator(task_id="ingest_dpe", python_callable=_run_dpe)


# ---------------------------------------------------------------------------
# SITADEL — mensuel (permis de construire, lag ~2 mois)
# ---------------------------------------------------------------------------

def _run_sitadel(**ctx):
    from include.ingestion.scripts.sitadel import run
    run()


with DAG(
    dag_id="ingestion_sitadel",
    description="Sit@del2 — Permis de construire (mensuel)",
    schedule="0 3 5 * *",  # 5 du mois à 03h00 (lag ~2 mois SDES)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["ingestion", "housing"],
) as dag_sitadel:
    PythonOperator(task_id="ingest_sitadel", python_callable=_run_sitadel)


# ---------------------------------------------------------------------------
# ECLN — trimestriel (commercialisation logements neufs)
# ---------------------------------------------------------------------------

def _run_ecln(**ctx):
    from include.ingestion.scripts.ecln import run
    run()


with DAG(
    dag_id="ingestion_ecln",
    description="ECLN — Commercialisation logements neufs (trimestriel)",
    schedule="0 3 15 1,4,7,10 *",  # 15 janv / avr / juil / oct à 03h00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["ingestion", "housing"],
) as dag_ecln:
    PythonOperator(task_id="ingest_ecln", python_callable=_run_ecln)


# ---------------------------------------------------------------------------
# EPTB — annuel (prix terrains et maisons neuves, publié ~mars)
# ---------------------------------------------------------------------------

def _run_eptb(**ctx):
    from include.ingestion.scripts.eptb import run
    run()


with DAG(
    dag_id="ingestion_eptb",
    description="EPTB — Prix terrains et maisons neuves (annuel)",
    schedule="0 5 1 3 *",  # 1er mars à 05h00
    start_date=datetime(2024, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["ingestion", "housing"],
) as dag_eptb:
    PythonOperator(task_id="ingest_eptb", python_callable=_run_eptb)


# ---------------------------------------------------------------------------
# RPLS — annuel (logements sociaux, millésime publié ~juillet)
# ---------------------------------------------------------------------------

def _run_rpls(**ctx):
    from include.ingestion.scripts.rpls import run
    run()


with DAG(
    dag_id="ingestion_rpls",
    description="RPLS — Répertoire des logements sociaux (annuel)",
    schedule="0 5 1 7 *",  # 1er juillet à 05h00
    start_date=datetime(2024, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["ingestion", "housing"],
) as dag_rpls:
    PythonOperator(task_id="ingest_rpls", python_callable=_run_rpls)
