{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(annee, code_departement, code_commune)',
        tags         = ['intermediate', 'sitadel']
    )
}}

-- Proxy de fct_construction exposé pour les jointures dans fct_transactions_enriched.

select * from {{ ref('fct_construction') }}
