# Housing Market Analytics

Pipeline de données et ML sur le marché immobilier français — centralise les sources publiques dans ClickHouse via Airflow + dbt.

## Stack

| Composant | Technologie |
|-----------|-------------|
| Orchestration | Apache Airflow 3.x (Astronomer) |
| Transformations | dbt Core 1.11 + dbt-clickhouse 1.9 + astronomer-cosmos 1.10 |
| Warehouse | ClickHouse 25.x |
| Runtime | Python 3.12, Docker Compose |
| ML (à venir) | XGBoost / LightGBM, PyTorch 2.x, SHAP |

## Sources de données

| Source | Fournisseur | API / Format | Fréquence | Lignes actuelles |
|--------|-------------|--------------|-----------|-----------------|
| DVF — Demandes de Valeurs Foncières | DGFiP | CSV.GZ streaming | Annuelle | ~8.8M |
| DPE — Diagnostics de Performance Énergétique | ADEME | REST (cursor pagination) | Continue | ~2.1M |
| ECLN — Commercialisation logements neufs | SDES/DiDo | REST (DiDo v1) | Trimestrielle | ~24k |
| RPLS — Répertoire des logements sociaux | SDES/DiDo | REST (DiDo v1) | Annuelle | ~58k |
| EPTB — Prix terrains et maisons neuves | SDES/DiDo | REST (DiDo v1) | Trimestrielle | ~600 |
| Sit@del2 — Permis de construire | SDES/DiDo | REST (DiDo v1) | Mensuelle | ~4.4k |
| INSEE — Revenus / démographie | INSEE | À intégrer | — | — |
| Banque de France — Taux immobiliers | BdF | À intégrer | — | — |

## Architecture

```
dags/
└── ingestion_housing.py     DAG mensuel d'ingestion (schedule: 1er du mois 04h00 UTC)

include/
└── ingestion/
    ├── base.py              Utilitaires partagés : client ClickHouse, watermarks,
    │                        retry HTTP (tenacity), pagineurs REST, chargeur DataFrame
    └── scripts/
        ├── dvf.py           Transactions immobilières (CSV streaming)
        ├── dpe.py           DPE ADEME (cursor pagination via champ "next")
        ├── rpls.py          Logements sociaux (DiDo)
        ├── ecln.py          Logements neufs (DiDo)
        ├── eptb.py          Prix terrains/maisons neuves (DiDo)
        ├── sitadel.py       Permis de construire (DiDo)
        ├── insee.py         (à compléter)
        └── macro.py         (à compléter)

dbt/                         Modèles dbt (staging → marts) via astronomer-cosmos
```

## DAG d'ingestion

Le DAG `ingestion_housing` orchestre le chargement incrémental de toutes les sources :

```
DVF ──┐
      ├── [parallèle]
DPE ──┘

RPLS → ECLN → EPTB → SITADEL  [séquentiel — rate-limit DiDo max 3 connexions/IP]
```

- **DVF + DPE** : APIs indépendantes, exécutées en parallèle
- **Sources DiDo** : enchaînées séquentiellement pour ne pas dépasser la limite de connexions
- **Retry** : 8 tentatives, backoff exponentiel 5 s → 120 s (HTTPError, ConnectionError, Timeout)

## Stratégie incrémentale et déduplication

### Watermarks (éviter les re-téléchargements)

Chaque source maintient un watermark dans `db_wh_housing._ingestion_watermarks`. À chaque run, le script ne télécharge que les données postérieures au dernier watermark connu.

| Source | Granularité watermark | Mise à jour |
|--------|----------------------|-------------|
| DPE | Date ISO (`YYYY-MM-DD`) | Après chaque page (10k lignes) |
| DVF | Année (`YYYY`) | Après chaque année complète |
| SITADEL | Mois (`YYYY-MM`) | Après le run complet |
| ECLN/EPTB | Trimestre (`YYYY-TN`) | Après le run complet |
| RPLS | Mois (`YYYY-MM`) | Après le run complet |

### ReplacingMergeTree (déduplication en cas de retry)

Toutes les tables `raw_*` utilisent `ENGINE = ReplacingMergeTree(_loaded_at)`. Si un retry Airflow réinsère des lignes déjà chargées, ClickHouse les déduplique automatiquement à la prochaine fusion (ou immédiatement avec `FINAL`).

**Clés de déduplication par table :**

| Table | ORDER BY (clé de dédup) |
|-------|------------------------|
| `raw_dvf` | `(annee, code_commune, id_mutation, id_parcelle, type_local)` |
| `raw_dpe` | `(annee, code_commune, numero_dpe)` |
| `raw_sitadel` | `(annee, mois, code_commune, type_logement)` |
| `raw_ecln` | `(annee, trimestre, code_departement, type_logement)` |
| `raw_eptb` | `(annee, trimestre, code_departement, type_logement)` |
| `raw_rpls` | `(annee, code_commune, financement)` |

> **Important pour les modèles dbt :** toujours utiliser `SELECT … FINAL` ou une vue avec `FINAL` pour requêter les tables `raw_*`, afin d'obtenir les données dédupliquées sans attendre la prochaine fusion de fond.

```sql
-- Exemple de requête correcte sur raw_dpe
SELECT * FROM db_wh_housing.raw_dpe FINAL WHERE annee = 2024;
```

## Démarrage local

### Prérequis

- Docker Desktop
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)

### Lancement

```bash
astro dev start
```

Airflow UI : http://localhost:8080 (admin / admin)

### Variables d'environnement requises

Définies dans `airflow_settings.yaml` ou via l'UI Airflow (connexion `clickhouse_default`) :

```
CLICKHOUSE_HOST      hôte ClickHouse (host.docker.internal en local)
CLICKHOUSE_PORT      8123 (HTTP)
CLICKHOUSE_USER      utilisateur ClickHouse
CLICKHOUSE_PASSWORD  mot de passe
```

### Tests

```bash
# Depuis le container scheduler
docker exec dbt-on-astro_<id>-scheduler-1 python -m pytest tests/ -v
```

243 tests — ingestion base, DVF, DPE, RPLS, ECLN, EPTB, Sit@del2.

## Watermarks actuels

| Source | Dernier chargement |
|--------|--------------------|
| dvf | 2025 (années 2021–2025) |
| dpe | 2022-07-18 (chargement en cours) |
| ecln | 2026-T1 (à jour) |
| rpls | 2025-01 (à jour) |
| eptb | — (données limitées) |
| sitadel | 2024-10 (chargement en cours) |
