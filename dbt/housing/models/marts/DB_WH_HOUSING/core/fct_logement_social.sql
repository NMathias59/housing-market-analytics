{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(annee, code_departement, code_commune)',
        tags         = ['core', 'fct']
    )
}}

-- Parc de logements sociaux par commune et millésime RPLS.
-- Source : RPLS (SDES/DiDo). Grain source : (annee, code_commune, financement).
-- Grain résultat : (annee, code_commune).

with source as (

    select * from {{ ref('stg_rpls') }}

),

final as (

    select
        annee,
        code_commune,
        code_departement,

        -- Volume et composition du parc
        count()                                             as nb_entrees_ls,
        countIf(financement = 'PLAI')                       as nb_plai,
        countIf(financement = 'PLUS')                       as nb_plus,
        countIf(financement = 'PLS')                        as nb_pls,

        -- Qualité énergétique
        countIf(classe_energie_dpe in ('F', 'G'))           as nb_passoires_ls,
        round(
            100.0 * countIf(classe_energie_dpe in ('F', 'G')) / count(),
            1
        )                                                   as pct_passoires_ls,

        -- Caractéristiques physiques
        round(avg(surface), 1)                              as surface_moy_ls,
        round(avg(nb_pieces), 1)                            as nb_pieces_moy_ls,
        round(avg(annee_construction), 0)                   as annee_construction_moy_ls

    from source
    group by annee, code_commune, code_departement

)

select * from final
