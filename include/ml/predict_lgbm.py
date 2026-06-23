"""
Walk-forward LightGBM — prédictions prix au m² par commune.
Stocke les prédictions et métriques de performance dans db_ai_house.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import mean_absolute_error, mean_squared_error

from include.ingestion.base import get_client as _ch_get_client

log = logging.getLogger(__name__)

TARGET       = 'prix_m2_med'
CAT_FEATURES = ['code_departement']
NUM_FEATURES = [
    'nb_transactions',
    'pct_appt', 'pct_maison', 'surface_moy', 'nb_pieces_moy',
    'pct_passoires_thermiques', 'conso_ep_moy',
    'nb_commences', 'taux_concretisation', 'nb_entrees_ls',
    'prix_m2_neuf_moy', 'delai_ecoulement_moy',
    'prix_m2_lag_1m', 'prix_m2_lag_3m', 'prix_m2_lag_6m', 'prix_m2_lag_12m',
    'prix_m2_roll3m', 'prix_m2_roll6m', 'prix_m2_roll12m',
    'evol_1m_pct', 'evol_12m_pct',
    'mois_sin', 'mois_cos',
]
ALL_FEATURES = CAT_FEATURES + NUM_FEATURES

LGBM_PARAMS = {
    'objective':         'regression',
    'metric':            'mae',
    'n_estimators':      1500,
    'learning_rate':     0.02,
    'num_leaves':        63,
    'min_child_samples': 20,
    'subsample':         0.8,
    'colsample_bytree':  0.8,
    'reg_alpha':         0.1,
    'reg_lambda':        1.0,
    'n_jobs':            -1,
    'verbose':           -1,
}

DDL_PREDS = """
CREATE TABLE IF NOT EXISTS db_ai_house.preds_ml_prix_m2
(
    run_id       String,
    code_commune String,
    annee_pred   UInt16,
    mois_pred    UInt8,
    prix_m2_pred Float32,
    prix_m2_reel Nullable(Float32),
    predicted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(predicted_at)
ORDER BY (run_id, code_commune, annee_pred, mois_pred)
"""

DDL_PERF = """
CREATE TABLE IF NOT EXISTS db_ai_house.model_perf_runs
(
    run_id      String,
    model_name  LowCardinality(String),
    run_date    DateTime,
    n_samples   UInt32,
    n_communes  UInt32,
    mae         Float32,
    rmse        Float32,
    mape        Nullable(Float32),
    horizon     UInt8,
    annee_test  UInt16,
    params      String,
    logged_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(logged_at)
ORDER BY (run_id, model_name, horizon)
"""


def _ensure_tables(client) -> None:
    client.command(DDL_PREDS)
    client.command(DDL_PERF)


def _load_features(client) -> pd.DataFrame:
    df = client.query_df("""
        SELECT
            code_commune, code_departement, annee, mois, prix_m2_med,
            nb_transactions, pct_appt, pct_maison, surface_moy, nb_pieces_moy,
            pct_passoires_thermiques, conso_ep_moy, nb_commences, taux_concretisation,
            nb_entrees_ls, prix_m2_neuf_moy, delai_ecoulement_moy,
            prix_m2_lag_1m, prix_m2_lag_3m, prix_m2_lag_6m, prix_m2_lag_12m,
            prix_m2_roll3m, prix_m2_roll6m, prix_m2_roll12m,
            evol_1m_pct, evol_12m_pct, mois_sin, mois_cos
        FROM db_ai_house.rpt_features_commune_mois
        WHERE prix_m2_med > 0
          AND prix_m2_lag_12m IS NOT NULL
        ORDER BY annee, mois, code_commune
    """)
    df['code_departement'] = df['code_departement'].astype('category')
    for col in NUM_FEATURES:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df


def _mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    mask = y_true > 0
    if not mask.any():
        return 0.0
    return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100)


def run(run_id: str | None = None) -> None:
    if run_id is None:
        run_id = f"lgbm_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    client = _ch_get_client()
    _ensure_tables(client)

    df = _load_features(client)
    annees = sorted(int(a) for a in df['annee'].unique())
    log.info('LGBM run_id=%s | %d lignes | %d communes | %d–%d',
             run_id, len(df), df['code_commune'].nunique(), annees[0], annees[-1])

    # Walk-forward : train sur tout < test_year, prédit test_year
    folds = [(annees[:i], annees[i]) for i in range(2, len(annees))]

    all_preds: list[pd.DataFrame] = []
    all_perf:  list[dict]         = []

    for train_years, test_year in folds:
        train = df[df['annee'].isin(train_years)].dropna(subset=[TARGET])
        test  = df[df['annee'] == test_year].dropna(subset=[TARGET])
        if len(train) < 200 or len(test) == 0:
            continue

        X_train, y_train = train[ALL_FEATURES], train[TARGET]
        X_test,  y_test  = test[ALL_FEATURES],  test[TARGET]

        model = lgb.LGBMRegressor(**LGBM_PARAMS)
        model.fit(
            X_train, y_train,
            categorical_feature=CAT_FEATURES,
            eval_set=[(X_test, y_test)],
            callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)],
        )
        preds = model.predict(X_test)

        mae  = float(mean_absolute_error(y_test, preds))
        rmse = float(np.sqrt(mean_squared_error(y_test, preds)))
        mape = _mape(y_test.values, preds)
        log.info('  fold test=%d | MAE=%.0f | RMSE=%.0f | MAPE=%.1f%%', test_year, mae, rmse, mape)

        pred_slice = test[['code_commune', 'annee', 'mois', TARGET]].copy()
        pred_slice['run_id']       = run_id
        pred_slice['prix_m2_pred'] = preds.astype('float32')
        pred_slice['prix_m2_reel'] = pred_slice[TARGET].astype('float32')
        all_preds.append(
            pred_slice[['run_id', 'code_commune', 'annee', 'mois', 'prix_m2_pred', 'prix_m2_reel']]
            .rename(columns={'annee': 'annee_pred', 'mois': 'mois_pred'})
        )

        all_perf.append({
            'run_id':     run_id,
            'model_name': 'lgbm',
            'run_date':   datetime.now(timezone.utc),
            'n_samples':  int(len(test)),
            'n_communes': int(test['code_commune'].nunique()),
            'mae':        mae,
            'rmse':       rmse,
            'mape':       mape,
            'horizon':    0,   # 0 = global (pas de multi-horizon pour LGBM)
            'annee_test': int(test_year),
            'params':     json.dumps(LGBM_PARAMS),
        })

    if all_preds:
        preds_df = pd.concat(all_preds, ignore_index=True)
        preds_df['prix_m2_reel'] = preds_df['prix_m2_reel'].astype('float32')
        client.insert_df('db_ai_house.preds_ml_prix_m2', preds_df)
        log.info('LGBM: %d prédictions insérées', sum(len(p) for p in all_preds))
    if all_perf:
        client.insert_df('db_ai_house.model_perf_runs', pd.DataFrame(all_perf))
        log.info('LGBM: %d lignes de perf insérées', len(all_perf))

    log.info('LGBM run_id=%s terminé', run_id)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()
