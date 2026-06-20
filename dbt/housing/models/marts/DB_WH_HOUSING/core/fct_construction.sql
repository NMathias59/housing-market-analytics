{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(annee, code_departement, code_commune)',
        tags         = ['core', 'fct']
    )
}}

-- Activité de construction neuve par commune et année (cumul 12 mois).
-- Source : Sit@del2 (SDES/DiDo), filtré sur 'Tous Logements' pour éviter les doublons.
-- Grain : (annee, code_commune).

with source as (

    select * from {{ ref('stg_sitadel') }}
    where type_logement = 'Tous Logements'

),

final as (

    select
        annee,
        code_commune,
        code_departement,

        sum(nb_logements_autorises)     as nb_autorises,
        sum(nb_logements_commences)     as nb_commences,
        sum(surface_autorisee)          as surface_autorisee,
        sum(surface_commencee)          as surface_commencee,

        round(
            100.0 * sum(nb_logements_commences) / nullif(sum(nb_logements_autorises), 0),
            1
        )                               as taux_concretisation

    from source
    group by annee, code_commune, code_departement

)

select * from final
