"""
LSTM bidirectionnel — prédictions multi-horizon t+1/t+2/t+3 par commune.
Stocke les prédictions et métriques de performance dans db_ai_house.

Architecture : BiLSTM(128) → BiLSTM(64) → Dense(64) → Dense(3)
Dénormalisation : par commune (scaler individuel) pour éviter les biais de zone.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

from include.ingestion.base import get_client as _ch_get_client

log = logging.getLogger(__name__)

LOOKBACK   = 12
HORIZON    = 3
TARGET_COL = 'prix_m2_med'

FEATURES = [
    'prix_m2_med',
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
TARGET_IDX = FEATURES.index(TARGET_COL)

DDL_PREDS = """
CREATE TABLE IF NOT EXISTS db_ai_house.preds_lstm_prix_m2
(
    run_id       String,
    code_commune String,
    annee_pred   UInt16,
    mois_pred    UInt8,
    horizon      UInt8,
    prix_m2_pred Float32,
    prix_m2_reel Nullable(Float32),
    predicted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(predicted_at)
ORDER BY (run_id, code_commune, annee_pred, mois_pred, horizon)
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


def _get_client():
    return _ch_get_client()


def _ensure_tables(client) -> None:
    client.command(DDL_PREDS)
    client.command(DDL_PERF)


def _load_features(client) -> pd.DataFrame:
    df = client.query_df("""
        SELECT code_commune, annee, mois, prix_m2_med, nb_transactions,
            pct_appt, pct_maison, surface_moy, nb_pieces_moy,
            pct_passoires_thermiques, conso_ep_moy, nb_commences, taux_concretisation,
            nb_entrees_ls, prix_m2_neuf_moy, delai_ecoulement_moy,
            prix_m2_lag_1m, prix_m2_lag_3m, prix_m2_lag_6m, prix_m2_lag_12m,
            prix_m2_roll3m, prix_m2_roll6m, prix_m2_roll12m,
            evol_1m_pct, evol_12m_pct, mois_sin, mois_cos
        FROM db_ai_house.rpt_features_commune_mois
        WHERE prix_m2_med > 0 AND prix_m2_lag_12m IS NOT NULL
        ORDER BY annee, mois, code_commune
    """)
    # Cast int32 pour éviter l'overflow uint16 lors du calcul de periode
    # (ex: uint16(2022) * 100 = 5492 au lieu de 202200)
    df['periode'] = df['annee'].astype('int32') * 100 + df['mois'].astype('int32')
    return df


def _fill_monthly_grid(raw: pd.DataFrame, commune: str) -> pd.DataFrame:
    """Complète les mois manquants et impute les valeurs nulles."""
    raw = raw[(raw['mois'].between(1, 12)) & (raw['annee'] > 2000)].copy()
    if raw.empty:
        return raw
    raw = raw.sort_values('periode')
    a, m = int(raw.iloc[0]['annee']),    int(raw.iloc[0]['mois'])
    a_e, m_e = int(raw.iloc[-1]['annee']), int(raw.iloc[-1]['mois'])
    rows = []
    while (a, m) <= (a_e, m_e):
        rows.append((a, m, a * 100 + m))
        m += 1
        if m > 12:
            m = 1
            a += 1
    grid = pd.DataFrame(rows, columns=['annee', 'mois', 'periode'])
    grid['code_commune'] = commune
    merged = grid.merge(
        raw.drop(columns=['code_commune', 'annee', 'mois'], errors='ignore'),
        on='periode', how='left',
    )
    merged[FEATURES] = merged[FEATURES].ffill().bfill().fillna(0)
    return merged


def _make_sequences(
    entity_df: pd.DataFrame,
    scaler: StandardScaler,
) -> tuple[np.ndarray, np.ndarray]:
    data = scaler.transform(entity_df[FEATURES].values.astype(np.float32))
    X, y = [], []
    for i in range(LOOKBACK, len(data) - HORIZON + 1):
        X.append(data[i - LOOKBACK:i])
        y.append(data[i:i + HORIZON, TARGET_IDX])
    return np.array(X, dtype=np.float32), np.array(y, dtype=np.float32)


def _build_model(n_features: int):
    from tensorflow import keras
    from tensorflow.keras import layers

    inp = keras.Input(shape=(LOOKBACK, n_features))
    x   = layers.Bidirectional(layers.LSTM(128, return_sequences=True))(inp)
    x   = layers.Dropout(0.2)(x)
    x   = layers.Bidirectional(layers.LSTM(64))(x)
    x   = layers.Dropout(0.2)(x)
    x   = layers.Dense(64, activation='relu')(x)
    out = layers.Dense(HORIZON)(x)
    m   = keras.Model(inp, out)
    m.compile(optimizer=keras.optimizers.Adam(1e-3), loss='mae', metrics=['mse'])
    return m


def run(run_id: str | None = None) -> None:
    if run_id is None:
        run_id = f"lstm_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    from tensorflow import keras

    client = _get_client()
    _ensure_tables(client)

    df = _load_features(client)
    annees    = sorted(int(a) for a in df['annee'].unique())
    test_year = annees[-1]
    val_year  = annees[-2] if len(annees) >= 2 else test_year - 1
    train_years = [a for a in annees if a < val_year]
    log.info('LSTM run_id=%s | train=%s val=%d test=%d', run_id, train_years, val_year, test_year)

    communes = sorted(df['code_commune'].unique())

    X_train_l, y_train_l = [], []
    X_val_l,   y_val_l   = [], []
    X_test_l              = []
    # meta_test: un dict par séquence de test pour la dénormalisation par commune
    meta_test: list[dict] = []
    scalers:   dict[str, StandardScaler] = {}

    for commune in communes:
        raw = df[df['code_commune'] == commune].sort_values('periode')
        if len(raw) < 12:
            continue
        commune_df = _fill_monthly_grid(raw, commune)
        if len(commune_df) < LOOKBACK + HORIZON + 6:
            continue

        all_train_df = commune_df[commune_df['annee'].isin(train_years)]
        if len(all_train_df) < LOOKBACK + HORIZON:
            continue

        scaler = StandardScaler()
        scaler.fit(all_train_df[FEATURES].values)
        scalers[commune] = scaler

        X, y = _make_sequences(all_train_df, scaler)
        if len(X):
            X_train_l.append(X)
            y_train_l.append(y)

        val_df  = commune_df[commune_df['annee'].isin(train_years[-1:] + [val_year])]
        Xv, yv  = _make_sequences(val_df, scaler)
        val_mask = val_df['annee'].values[LOOKBACK:len(val_df) - HORIZON + 1] == val_year
        if val_mask.any() and len(Xv):
            X_val_l.append(Xv[val_mask])
            y_val_l.append(yv[val_mask])

        test_df     = commune_df[commune_df['annee'].isin([val_year, test_year])]
        Xt, yt      = _make_sequences(test_df, scaler)
        test_df_pos = test_df.reset_index(drop=True)
        test_mask   = test_df_pos['annee'].values[LOOKBACK:len(test_df_pos) - HORIZON + 1] == test_year
        if test_mask.any() and len(Xt):
            X_test_l.append(Xt[test_mask])
            for k in np.where(test_mask)[0]:
                horizon_targets = []
                for h in range(HORIZON):
                    pos = LOOKBACK + k + h
                    if pos < len(test_df_pos):
                        row = test_df_pos.iloc[pos]
                        reel = float(row[TARGET_COL]) if row[TARGET_COL] > 0 else None
                        horizon_targets.append({
                            'annee': int(row['annee']),
                            'mois':  int(row['mois']),
                            'reel':  reel,
                        })
                meta_test.append({'commune': commune, 'scaler': scaler, 'targets': horizon_targets})

    if not X_train_l:
        raise RuntimeError('LSTM: aucune commune retenue — vérifier les données ClickHouse')

    X_train = np.concatenate(X_train_l)
    y_train = np.concatenate(y_train_l)
    X_val   = np.concatenate(X_val_l)  if X_val_l  else np.empty((0, LOOKBACK, len(FEATURES)), np.float32)
    y_val   = np.concatenate(y_val_l)  if y_val_l  else np.empty((0, HORIZON),                 np.float32)
    X_test  = np.concatenate(X_test_l) if X_test_l else np.empty((0, LOOKBACK, len(FEATURES)), np.float32)
    log.info('Shapes — Train=%s Val=%s Test=%s', X_train.shape, X_val.shape, X_test.shape)

    val_data = (X_val, y_val) if len(X_val) else None
    # Adapter le monitor selon la disponibilité des données de validation
    monitor = 'val_loss' if val_data is not None else 'loss'
    callbacks = [
        keras.callbacks.EarlyStopping(patience=15, restore_best_weights=True, monitor=monitor),
        keras.callbacks.ReduceLROnPlateau(factor=0.5, patience=7, min_lr=1e-5, monitor=monitor),
    ]
    model = _build_model(X_train.shape[2])
    model.fit(
        X_train, y_train,
        validation_data=val_data,
        epochs=150, batch_size=256,
        callbacks=callbacks, verbose=0,
    )

    y_pred_norm = model.predict(X_test, verbose=0)

    # Reconstruction des prédictions avec dénormalisation par commune
    pred_rows: list[dict] = []
    perf_by_h: dict[int, dict] = {h: {'abs_err': [], 'sq_err': []} for h in range(HORIZON)}

    for seq_i, meta in enumerate(meta_test):
        sc    = meta['scaler']
        scale = sc.scale_[TARGET_IDX]
        mean_ = sc.mean_[TARGET_IDX]
        for h, tgt in enumerate(meta['targets']):
            pred = float(y_pred_norm[seq_i, h]) * scale + mean_
            reel = tgt['reel']
            pred_rows.append({
                'run_id':       run_id,
                'code_commune': meta['commune'],
                'annee_pred':   tgt['annee'],
                'mois_pred':    tgt['mois'],
                'horizon':      h + 1,
                'prix_m2_pred': pred,
                'prix_m2_reel': reel,
            })
            if reel is not None:
                perf_by_h[h]['abs_err'].append(abs(pred - reel))
                perf_by_h[h]['sq_err'].append((pred - reel) ** 2)

    perf_rows: list[dict] = []
    for h in range(HORIZON):
        abs_err = perf_by_h[h]['abs_err']
        sq_err  = perf_by_h[h]['sq_err']
        mae  = float(np.mean(abs_err))  if abs_err else 0.0
        rmse = float(np.sqrt(np.mean(sq_err))) if sq_err else 0.0
        log.info('LSTM t+%d: MAE=%.0f RMSE=%.0f (n=%d)', h + 1, mae, rmse, len(abs_err))
        perf_rows.append({
            'run_id':     run_id,
            'model_name': 'lstm',
            'run_date':   datetime.now(timezone.utc),
            'n_samples':  len(X_test),
            'n_communes': len(scalers),
            'mae':        mae,
            'rmse':       rmse,
            'mape':       None,
            'horizon':    h + 1,
            'annee_test': int(test_year),
            'params':     json.dumps({
                'lookback':     LOOKBACK,
                'horizon':      HORIZON,
                'architecture': 'BiLSTM_128_64_Dense_64',
                'train_years':  train_years,
            }),
        })

    preds_df = pd.DataFrame(pred_rows)
    # Forcer float32 sur Nullable(Float32) — évite dtype object avec des None mélangés à des float
    preds_df['prix_m2_reel'] = pd.to_numeric(preds_df['prix_m2_reel'], errors='coerce').astype('float32')

    perf_df = pd.DataFrame(perf_rows)
    perf_df['mape'] = np.nan  # Nullable(Float32) : np.nan → NULL, None → dtype object (erreur CH)

    client.insert_df('db_ai_house.preds_lstm_prix_m2', preds_df)
    client.insert_df('db_ai_house.model_perf_runs',    perf_df)
    log.info('LSTM run_id=%s: %d preds, %d perf rows', run_id, len(pred_rows), len(perf_rows))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()
