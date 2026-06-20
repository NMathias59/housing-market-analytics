{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(annee, code_departement, code_commune)',
        tags         = ['intermediate', 'rpls']
    )
}}

-- Proxy de fct_logement_social exposé pour les jointures dans fct_transactions_enriched.

select * from {{ ref('fct_logement_social') }}
