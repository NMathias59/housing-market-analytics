{{
    config(
        schema       = 'db_ai_house',
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(code_departement, annee, mois)',
        tags         = ['ai', 'shap']
    )
}}

-- Échantillon stratifié pour l'analyse SHAP.
-- ~5 % de fct_features_commune_mois, sélection déterministe par hash.
-- Filtres qualité : uniquement les lignes avec historique complet (lag 12m non null)
-- et un volume de transactions suffisant pour que le prix soit fiable.
-- Export attendu : parquet via notebooks Python (projet SHAP séparé).

select *
from {{ ref('rpt_features_commune_mois') }}
where
    prix_m2_lag_12m  is not null
    and nb_transactions  >= 5
    and prix_m2_moy      > 0
    and cityHash64(code_commune, annee, mois) % 20 = 0
