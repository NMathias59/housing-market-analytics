{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(annee)',
        tags         = ['core', 'fct']
    )
}}

-- Prix des terrains à bâtir et des maisons neuves par zone géographique et année.
-- Source : EPTB (SDES/DiDo). Grain : (annee, zone_code).

select * from {{ ref('stg_eptb') }}
