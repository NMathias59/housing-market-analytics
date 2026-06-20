{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(annee, trimestre, code_departement)',
        tags         = ['core', 'fct']
    )
}}

-- Commercialisation de logements neufs par département et trimestre.
-- Source : ECLN (SDES/DiDo), filtré sur 'Tous logements'.
-- Grain : (annee, trimestre, code_departement).

with source as (

    select * from {{ ref('stg_ecln') }}
    where type_logement = 'Tous logements'

),

final as (

    select
        annee,
        trimestre,
        code_departement,

        nb_mises_en_vente,
        nb_reservations,
        nb_annulations,
        stock,
        delai_ecoulement,
        prix_m2                         as prix_m2_neuf,
        prix_moyen_individuel,

        round(
            100.0 * nb_reservations / nullif(nb_mises_en_vente, 0),
            1
        )                               as taux_reservation

    from source

)

select * from final
