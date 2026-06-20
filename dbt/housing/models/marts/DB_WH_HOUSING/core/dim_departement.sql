{{
    config(
        materialized = 'table',
        engine       = 'ReplacingMergeTree()',
        order_by     = '(code_departement)',
        tags         = ['core', 'dim']
    )
}}

-- Référentiel des départements français.
-- Dérivé de stg_ecln qui fournit les libellés officiels SDES.

select
    code_departement,
    any(libelle_departement) as libelle_departement
from {{ ref('stg_ecln') }}
where code_departement != ''
group by code_departement
