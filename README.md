# Housing Market Analytics

Pipeline de données et ML de bout en bout sur le marché immobilier français.
Ingestion multi-sources → warehouse ClickHouse → transformations dbt → modèles prédictifs (LightGBM + LSTM) → analyse SHAP des drivers de prix.

> **Contexte entretien** : projet personnel construit pour couvrir l'ensemble du cycle analytique — data engineering, modélisation dimensionnelle, feature engineering, ML supervisé et interprétabilité. Toutes les données sont publiques et open data.

---

## Ce que ce projet démontre

| Compétence | Preuve concrète |
|---|---|
| Data Engineering | Ingestion 6 sources hétérogènes (CSV streaming, REST paginé, cursor), watermarks, retry idempotent |
| Data Modeling | 22 modèles dbt — staging / intermediate / marts — avec déduplication ReplacingMergeTree |
| Orchestration | DAG Airflow 3 avec Cosmos (dbt natif), parallélisme, gestion rate-limit API |
| Feature Engineering | Time series par commune : 24 features, lags 1m/3m/6m/12m, rolling means, encodage cyclique |
| ML supervisé | LightGBM walk-forward (MAE 330 €/m²) + BiLSTM multi-horizon t+1/t+2/t+3 |
| Interprétabilité | SHAP sur 13 931 communes — top driver : département (490 €/m², 40 % de la variance) |
| Infra locale | Stack dockerisée reproductible : Astro CLI + ClickHouse + dbt en un `astro dev start` |

---

## Stack technique

| Couche | Technologie | Pourquoi ce choix |
|---|---|---|
| Orchestration | Apache Airflow 3 (Astronomer Astro) | Standard industrie, Cosmos pour l'intégration dbt native |
| Transformations | dbt Core 1.11 + dbt-clickhouse 1.9 | SQL versionné, tests, lineage, documentation auto |
| Warehouse | ClickHouse 25.x | OLAP columnar, ReplacingMergeTree pour l'idempotence, excellent sur les agrégations temporelles |
| ML | LightGBM 4 · TensorFlow CPU 2.x | GBDT pour le baseline explicable, LSTM pour la dépendance temporelle longue |
| Interprétabilité | SHAP | Valeurs de Shapley — seul framework model-agnostic rigoureux |
| Runtime | Python 3.12 · Docker Compose · Astro CLI | Reproductibilité locale complète |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                       Sources publiques                          │
│  DVF (DGFiP)  DPE (ADEME)  Sit@del2  ECLN  EPTB  RPLS (SDES)   │
└──────┬─────────────┬──────────────────────────────────────────────┘
       │             │   Airflow DAG — ingestion_housing
       │  [parallèle]│   [séquentiel — rate-limit DiDo]
       ▼             ▼
┌─────────────────────────┐
│   ClickHouse            │
│   db_wh_housing         │   raw_dvf · raw_dpe · raw_sitadel
│   (tables raw_*)        │   raw_ecln · raw_eptb · raw_rpls
└──────────┬──────────────┘
           │  dbt (via Cosmos)
           ▼
┌─────────────────────────┐
│   ClickHouse            │
│   db_wh_housing         │   staging → intermediate → core
│   (marts analytiques)   │   fct_transactions · fct_dpe_commune
│                         │   fct_construction · fct_logement_social
│   db_ai_house           │   rpt_features_commune_mois (ML)
│   (data mart ML)        │   rpt_shap_commune · rpt_shap_sample
└──────────┬──────────────┘
           │
    ┌──────┴──────┐
    ▼             ▼
┌────────┐  ┌──────────┐
│ LGBM   │  │  BiLSTM  │   Airflow DAGs — predict_housing_ml
│ walk-  │  │  BiDir   │                  predict_housing_lstm
│forward │  │ t+1/2/3  │
└────────┘  └──────────┘
    └──────┬──────┘
           ▼
┌─────────────────────────┐
│   db_ai_house           │   preds_ml_prix_m2
│   (prédictions)         │   preds_lstm_prix_m2
│                         │   model_perf_runs
└─────────────────────────┘
           │
           ▼
  notebooks/shap — Analyse SHAP (interprétabilité)
```

---

## Sources de données

| Source | Fournisseur | Volume | Ingestion | Fréquence |
|---|---|---|---|---|
| DVF — transactions immobilières | DGFiP | ~8.8M lignes | CSV.GZ streaming par année | Annuelle |
| DPE — diagnostics énergétiques | ADEME | ~2.1M lignes | REST — cursor pagination `next` | Continue |
| Sit@del2 — permis de construire | SDES/DiDo | ~28.5M lignes | CSV millesime streaming | Mensuelle |
| ECLN — logements neufs | SDES/DiDo | ~24k lignes | REST DiDo v1 (page/pageSize) | Trimestrielle |
| EPTB — prix terrains neufs | SDES/DiDo | ~600 lignes | REST DiDo v1 | Trimestrielle |
| RPLS — logements sociaux | SDES/DiDo | ~58k lignes | CSV millesime | Annuelle |

### Ingestion — points techniques notables

- **Streaming sans chargement en mémoire** : DVF et Sit@del2 (fichiers > 1 Go) sont décompressés et lus en chunks pandas directement sur le flux HTTP, sans écriture disque intermédiaire.
- **Pagination cursor ADEME** : l'API ADEME plafonne à 10 000 lignes en pagination offset. La solution utilise le champ `next` de la réponse pour une pagination cursor illimitée.
- **Rate-limit DiDo** : les 4 sources DiDo sont exécutées en séquence dans le DAG pour éviter le code 429. DVF et DPE sont en parallèle (APIs indépendantes).
- **Retry idempotent** : 8 tentatives HTTP avec backoff exponentiel (5 s → 120 s). Un retry Airflow ne crée pas de doublons grâce au ReplacingMergeTree.

---

## Idempotence et déduplication

### Watermarks

Chaque source maintient un watermark dans `db_wh_housing._ingestion_watermarks` (ReplacingMergeTree).

| Source | Clé watermark | Logique |
|---|---|---|
| DVF | Année (`2025`) | Ne charge que les années non encore vues |
| DPE | Date (`2022-07-18`) | Charge depuis le dernier `date_etablissement_dpe` |
| Sit@del2 | Mois + millesime | Skip si le millesime du mois est déjà en base |
| ECLN / EPTB | Trimestre (`2026-T1`) | Skip si le trimestre est déjà chargé |
| RPLS | Millesime annuel | Skip si le millesime est déjà chargé |

### ReplacingMergeTree

Toutes les tables `raw_*` utilisent `ReplacingMergeTree(_loaded_at)`. En cas de rechargement (retry, backfill), les doublons sont écrasés à la fusion ou éliminés avec `SELECT … FINAL` dans dbt.

---

## Modélisation dbt — 22 modèles

```
staging/          Vues 1-to-1 sur les tables raw_* — casting, renommage, FINAL
                  stg_dvf · stg_dpe · stg_sitadel · stg_ecln · stg_eptb · stg_rpls

intermediate/     Agrégations intermédiaires par commune × période
                  int_prix_commune_mois · int_dpe_commune · int_construction_commune

marts/
  DB_WH_HOUSING/
    core/         Tables de faits analytiques (grain : commune × mois ou trimestre)
                  fct_transactions · fct_dpe_commune · fct_construction · fct_logement_social

  DB_AI_HOUSE/
    rpt_features_commune_mois   Time series — grain commune × mois, 24 features
    rpt_shap_commune            Cross-section — grain commune (moyenne 2021-2025)
    rpt_shap_sample             5 % temporellement stratifié pour SHAP sur grand volume
```

### Feature engineering dans dbt (`rpt_features_commune_mois`)

- **Lags temporels** : `prix_m2_lag_1m / 3m / 6m / 12m` — via `lagInFrame` ClickHouse sur `PARTITION BY code_commune ORDER BY periode`
- **Rolling means** : `prix_m2_roll3m / 6m / 12m` — moyennes glissantes sur fenêtre variable
- **Évolutions** : `evol_1m_pct`, `evol_12m_pct` — variation relative mois sur mois et annuelle
- **Encodage cyclique** : `mois_sin = sin(2π × mois / 12)`, `mois_cos` — évite la rupture artificielle décembre → janvier
- **Jointure multi-sources** : DVF (prix), DPE (énergie), Sit@del2 (construction), ECLN (neuf), RPLS (social) — alignés sur la clé `code_commune × annee × mois`

---

## Pipelines ML

### LightGBM — walk-forward validation

**Principe** : pour chaque année test, le modèle est entraîné exclusivement sur les années précédentes. Aucune fuite temporelle.

```
train=[2022, 2023]  →  test=2024   MAE=343 €/m²   RMSE=7 156
train=[2022–2024]   →  test=2025   MAE=319 €/m²   RMSE=4 862
```

- 386 433 lignes, 17 632 communes, 24 features
- `code_departement` en feature catégorielle native LightGBM
- Early stopping sur 50 rounds (évalue sur le fold test pour arrêter avant overfit)

### LSTM BiDirectionnel — prédictions multi-horizon

**Architecture** : `BiLSTM(128) → Dropout(0.2) → BiLSTM(64) → Dropout(0.2) → Dense(64) → Dense(3)`

- **Input** : fenêtre glissante de 12 mois (lookback=12), 24 features normalisées par commune
- **Output** : prédictions simultanées t+1, t+2, t+3 (prix/m² pour les 3 mois suivants)
- **Normalisation par commune** : un `StandardScaler` par commune — évite que les biais de zone géographique contaminent la normalisation
- **Entraînement** : 150 epochs max, EarlyStopping(patience=15) + ReduceLROnPlateau(patience=7)

```
Train=(65 798 séquences)  Val=(89 861)  Test=(86 215)

t+1 :  MAE=1 048 €/m²   RMSE=8 512
t+2 :  MAE=1 008 €/m²   RMSE=7 152
t+3 :  MAE=  936 €/m²   RMSE=8 288
```

### Comparaison des modèles

| Modèle | MAE test | Horizon | Points forts |
|---|---|---|---|
| LightGBM walk-forward | **330 €/m²** | 0 (même mois) | Explicable, rapide, robuste aux outliers |
| LSTM BiDir | **~1 000 €/m²** | t+1/t+2/t+3 | Capture la dépendance temporelle longue, prédictions futures |

> Le LSTM est moins précis que LightGBM sur le même mois (attendu : il prédit le futur sans voir les données de ce mois), mais c'est le seul modèle capable de produire des prédictions sur les 3 mois à venir.

---

## Analyse SHAP — drivers structurels du prix

Sur 13 931 communes (cross-section 2021-2025) :

| Rang | Feature | Impact moyen | Part de variance |
|---|---|---|---|
| 1 | Département (localisation) | 490 €/m² | 39.5 % |
| 2 | % maisons (caractère rural) | 110 €/m² | 8.9 % |
| 3 | Volume de transactions (liquidité) | 107 €/m² | 8.6 % |
| 4 | Nombre de pièces moyen | 97 €/m² | 7.8 % |
| 5 | Surface moyenne | 70 €/m² | 5.6 % |
| 6 | % appartements (urbanité) | 49 €/m² | 3.9 % |
| 7 | Logements mis en chantier | 43 €/m² | 3.5 % |

**Lecture** : la localisation départementale explique à elle seule 40 % de la variance du prix au m². Les features physiques du bien (surface, pièces, type) expliquent environ 22 % combinés. Le marché local (transactions, construction) compte pour ~12 %.

---

## Décisions d'architecture

**Pourquoi ClickHouse plutôt que PostgreSQL ou BigQuery ?**
Les requêtes analytiques (agrégations sur des millions de lignes, fenêtres temporelles par commune) sont le cas d'usage natif d'un moteur OLAP columnar. ClickHouse est gratuit, auto-hébergeable, et ses fonctions de fenêtrage (`lagInFrame`, `avgInFrame`) simplifient le feature engineering directement en SQL.

**Pourquoi ReplacingMergeTree pour l'ingestion ?**
L'ingestion est exécutée mensuellement via Airflow, avec des retries automatiques. ReplacingMergeTree garantit qu'un retry ne crée pas de doublons sans nécessiter de logique `UPSERT` côté applicatif — la déduplication est déléguée au moteur de stockage.

**Pourquoi Cosmos pour dbt dans Airflow ?**
Cosmos parse le projet dbt et crée un sous-DAG Airflow par modèle, avec les dépendances correctes. L'alternative (un seul `dbt build` en PythonOperator) masque les erreurs individuelles et empêche le retry fin.

**Pourquoi walk-forward plutôt qu'un split train/test classique ?**
Les données immobilières ont une forte autocorrélation temporelle. Un split aléatoire entraînerait une fuite de données (data leakage) — le modèle verrait des données futures pendant l'entraînement. La validation walk-forward reproduit les conditions réelles de déploiement.

**Pourquoi le LSTM prédit t+1/t+2/t+3 simultanément ?**
Une architecture multi-output (un Dense(3) en sortie) est plus stable qu'un modèle par horizon. Elle force le modèle à apprendre une représentation cohérente sur les 3 horizons plutôt que d'overfitter sur chacun séparément.

---

## Structure du projet

```
dags/
├── ingestion_housing.py     DAG mensuel — ingestion 6 sources + dbt build
├── predict_housing_ml.py    DAG ML — walk-forward LightGBM
└── predict_housing_lstm.py  DAG ML — BiLSTM multi-horizon

include/
├── ingestion/
│   ├── base.py              Client ClickHouse, watermarks, retry HTTP, pagineurs
│   └── scripts/             dvf · dpe · sitadel · ecln · eptb · rpls
└── ml/
    ├── predict_lgbm.py      Walk-forward LightGBM + insert ClickHouse
    └── predict_lstm.py      BiLSTM BiDir + normalisation par commune + insert ClickHouse

dbt/housing/models/
├── staging/                 6 modèles — nettoyage et casting
├── intermediate/            Agrégations intermédiaires
└── marts/
    ├── DB_WH_HOUSING/core/  4 tables de faits analytiques
    └── DB_AI_HOUSE/         3 datasets ML/SHAP

notebooks/
├── shap/shap_prix_m2.ipynb           Analyse SHAP drivers de prix
├── ml_baseline/ml_prix_m2.ipynb      Exploration LightGBM
└── dl_timeseries/dl_lstm_prix_m2.ipynb  Prototypage LSTM
```

---

## Démarrage local

**Prérequis** : Docker Desktop + Astro CLI

```bash
# Démarrer la stack Airflow complète
astro dev start

# Airflow UI → http://localhost:8080  (admin / admin)
```

```bash
# Démarrer ClickHouse séparément si nécessaire
docker compose -f docker-compose.clickhouse.yml up -d
```

```bash
# Notebooks (depuis l'hôte)
pip install lightgbm shap scikit-learn clickhouse-connect jupyter
jupyter lab notebooks/
```

**Variables `.env`** (pour les notebooks locaux) :
```
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=admin
CLICKHOUSE_PASSWORD=***
```

> Les containers Airflow utilisent `host.docker.internal` au lieu de `localhost` (configuré dans `docker-compose.override.yml`).

---

## Watermarks actuels

| Source | Dernier chargement |
|---|---|
| dvf | 2025 (années 2021–2025, ~8.8M lignes) |
| dpe | En cours depuis 2021 |
| ecln | 2026-T1 (à jour) |
| rpls | 2025-01 (à jour) |
| eptb | Données limitées (~600 lignes) |
| sitadel | En cours depuis 2024-11 |
