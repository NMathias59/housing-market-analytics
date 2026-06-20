{{
    config(
        materialized = 'table',
        engine       = 'ReplacingMergeTree()',
        order_by     = '(code_commune)',
        tags         = ['core', 'dim']
    )
}}

-- Référentiel des communes françaises.
-- Dérivé de stg_dvf (source principale nom/dept/postal) enrichi de code_region via stg_rpls.

with communes as (

    select
        code_commune,
        any(nom_commune)        as nom_commune,
        any(code_departement)   as code_departement,
        any(code_postal)        as code_postal
    from {{ ref('stg_dvf') }}
    where code_commune != ''
    group by code_commune

),

regions as (

    select
        code_commune,
        any(code_region) as code_region
    from {{ ref('stg_rpls') }}
    where code_commune != ''
    group by code_commune

)

select
    c.code_commune,
    c.nom_commune,
    c.code_departement,
    c.code_postal,
    r.code_region
from communes c
left join regions r using (code_commune)
