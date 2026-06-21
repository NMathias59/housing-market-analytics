# Housing Market Analytics

Pipeline de données et ML sur le marché immobilier français — centralise les sources publiques dans ClickHouse via Airflow + dbt, et expose des datasets prêts pour le deep learning et l'analyse SHAP.

## Stack

| Composant | Technologie |
|-----------|-------------|
| Orchestration | Apache Airflow 3.x (Astronomer) |
| Transformations | dbt Core 1.11 + dbt-clickhouse 1.9 + astronomer-cosmos 1.10 |
| Warehouse | ClickHouse 25.x |
| Runtime | Python 3.12, Docker Compose |
| ML | LightGBM, PyTorch 2.x (LSTM / Transformer), SHAP |

## Sources de données

| Source | Fournisseur | Format | Fréquence | Lignes |
|--------|-------------|--------|-----------|--------|
| DVF — Demandes de Valeurs Foncières | DGFiP | CSV.GZ streaming | Annuelle | ~8.8M |
| DPE — Diagnostics de Performance Énergétique | ADEME | REST cursor pagination | Continue | ~2.1M |
| Sit@del2 — Permis de construire | SDES/DiDo | CSV millesime streaming | Mensuelle | ~28.5M |
| ECLN — Commercialisation logements neufs | SDES/DiDo | REST DiDo v1 | Trimestrielle | ~24k |
| RPLS — Répertoire des logements sociaux | SDES/DiDo | CSV millesime | Annuelle | ~58k |
| EPTB — Prix terrains et maisons neuves | SDES/DiDo | REST DiDo v1 | Trimestrielle | ~600 |
| INSEE — Revenus / démographie | INSEE | À intégrer | — | — |
| Banque de France — Taux immobiliers | BdF | À intégrer | — | — |

## Architecture

```
dags/
└── ingestion_housing.py     DAG mensuel (schedule: 7 du mois 04h00 UTC)

include/
└── ingestion/
    ├── base.py              Client ClickHouse, watermarks, retry HTTP,
    │                        pagineurs REST, chargeur DataFrame
    └── scripts/
        ├── dvf.py           Transactions immobilières (CSV streaming annuel)
        ├── dpe.py           DPE ADEME (cursor pagination)
        ├── sitadel.py       Permis de construire (CSV millesime DiDo)
        ├── rpls.py          Logements sociaux (CSV millesime DiDo)
        ├── ecln.py          Logements neufs (REST DiDo)
        └── eptb.py          Prix terrains/maisons neuves (REST DiDo)

dbt/housing/models/
├── staging/                 Vues nettoyées sur les tables raw_*
├── intermediate/            Agrégations intermédiaires
└── marts/
    ├── DB_WH_HOUSING/       Warehouse analytique
    │   └── core/            fct_transactions, fct_dpe_commune,
    │                        fct_construction, fct_logement_social...
    └── DB_AI_HOUSE/         Data mart ML (self-service)
        ├── rpt_features_commune_mois.sql   Time series commune × mois
        ├── rpt_shap_sample.sql             Sample 5% pour SHAP (temporel)
        └── rpt_shap_commune.sql            Dataset cross-sectionnel SHAP

notebooks/
├── shap/
│   └── shap_prix_m2.ipynb  Analyse SHAP des drivers structurels du prix
├── exploration/
└── inference/
```

## DAG d'ingestion

```
DVF ──┐
      ├── [parallèle]
DPE ──┘

RPLS → ECLN → EPTB → SITADEL  [séquentiel — rate-limit DiDo]
                         │
                    retry quotidien (14j) si millesime pas encore publié
                    └──▶ dbt build (staging → marts)
```

- **DVF + DPE** : APIs indépendantes, exécutées en parallèle
- **Sources DiDo** : séquentielles pour ne pas dépasser le rate-limit 429
- **Sit@del2** : download CSV millesime en streaming (1 requête vs 285 000 appels paginés)
- **Retry** : 8 tentatives HTTP, backoff 5 s → 120 s. Sit@del2 : retry quotidien jusqu'au 15 du mois si le millesime n'est pas encore publié

## Stratégie incrémentale et déduplication

### Watermarks

Chaque source maintient un watermark dans `db_wh_housing._ingestion_watermarks`.

| Source | Granularité | Particularité |
|--------|-------------|---------------|
| DPE | Date (`YYYY-MM-DD`) | Cursor pagination ADEME |
| DVF | Année (`YYYY`) | Fichier annuel complet |
| SITADEL | Mois (`YYYY-MM`) + millesime | CSV millesime, skip si déjà chargé |
| ECLN/EPTB | Trimestre (`YYYY-TN`) | |
| RPLS | Millesime (`YYYY-MM`) | CSV par millesime annuel |

### ReplacingMergeTree

Toutes les tables `raw_*` utilisent `ENGINE = ReplacingMergeTree(_loaded_at)`. Les retries Airflow ne créent pas de doublons — ClickHouse déduplique à la fusion ou avec `FINAL`.

| Table | ORDER BY |
|-------|----------|
| `raw_dvf` | `(annee, code_commune, id_mutation, id_parcelle, type_local)` |
| `raw_dpe` | `(annee, code_commune, numero_dpe)` |
| `raw_sitadel` | `(annee, mois, code_commune, type_logement)` |
| `raw_ecln` | `(annee, trimestre, code_departement, type_logement)` |
| `raw_eptb` | `(annee, trimestre, code_departement, type_logement)` |
| `raw_rpls` | `(annee, code_commune, financement)` |

## Data mart ML — DB_AI_HOUSE

### `rpt_features_commune_mois`
Time series pour LSTM / Transformer. Grain : commune × mois.
- Target : `prix_m2_moy`, `med`, `p10`, `p90`
- Features DVF, DPE, Sit@del2, RPLS, ECLN
- Lags : `prix_m2_lag_1m/3m/6m/12m`, rolling 3m/6m/12m, `evol_1m_pct`, `evol_12m_pct`
- Encodage cyclique du mois (`mois_sin`, `mois_cos`)

### `rpt_shap_commune`
Dataset cross-sectionnel pour l'analyse SHAP. Grain : commune (moyennée 2021-2025).
- Une ligne par commune, pas de dimension temporelle
- `code_departement` en catégorielle (driver #1 du prix : 40% de la variance)

### Résultats SHAP (`notebooks/shap/shap_prix_m2.ipynb`)

Top drivers structurels du prix au m² sur 13 931 communes :

| Rang | Variable | Impact moyen | Part |
|------|----------|-------------|------|
| 1 | Département (localisation) | 490 €/m² | 39.5% |
| 2 | % maisons (caractère rural) | 110 €/m² | 8.9% |
| 3 | Volume de transactions (liquidité) | 107 €/m² | 8.6% |
| 4 | Nombre de pièces moyen | 97 €/m² | 7.8% |
| 5 | Surface moyenne des biens | 70 €/m² | 5.6% |
| 6 | % appartements (urbanité) | 49 €/m² | 3.9% |
| 7 | Logements mis en chantier | 43 €/m² | 3.5% |

## Démarrage local

```bash
# Prérequis : Docker Desktop + Astro CLI
astro dev start
```

Airflow UI : http://localhost:8080 (admin / admin)

Variables d'environnement dans `.env` :
```
CLICKHOUSE_HOST      host.docker.internal (en local)
CLICKHOUSE_PORT      8123
CLICKHOUSE_USER      admin
CLICKHOUSE_PASSWORD  ***
```

### Notebooks

```bash
pip install lightgbm shap scikit-learn clickhouse-connect python-dotenv
jupyter lab notebooks/shap/shap_prix_m2.ipynb
```

### Tests

```bash
docker exec <scheduler-container> python -m pytest tests/ -v
```

## Watermarks actuels

| Source | Dernier chargement |
|--------|--------------------|
| dvf | 2025 (années 2021–2025) |
| dpe | 2022-07-18 (chargement en cours) |
| ecln | 2026-T1 (à jour) |
| rpls | 2025-01 (à jour) |
| eptb | — (données limitées) |
| sitadel | 2024-10 (reprise via CSV millesime) |
