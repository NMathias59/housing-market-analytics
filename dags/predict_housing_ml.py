"""
DAG predict_housing_ml — Prédictions LightGBM walk-forward prix au m².
Déclenché par le DAG ingestion_housing après la transformation dbt.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator


def _run_lgbm(**ctx):
    import sys

    stale = [k for k in sys.modules if k.startswith('include.')]
    for k in stale:
        del sys.modules[k]

    from include.ml.predict_lgbm import run

    run_id = f"lgbm_{ctx.get('ds_nodash', datetime.utcnow().strftime('%Y%m%d'))}"
    run(run_id=run_id)


with DAG(
    dag_id='predict_housing_ml',
    description='Prédictions LightGBM walk-forward prix au m² → db_ai_house',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries':     1,
        'retry_delay': timedelta(minutes=15),
    },
    tags=['ml', 'housing', 'predictions'],
) as dag:

    PythonOperator(
        task_id='run_lgbm_predictions',
        python_callable=_run_lgbm,
    )
