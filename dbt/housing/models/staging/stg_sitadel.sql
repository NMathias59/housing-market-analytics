{{
    config(
        materialized = 'view',
        tags = ['staging', 'sitadel']
    )
}}

with source as (

    select * from {{ source('raw', 'raw_sitadel') }} final

),

cleaned as (

    select
        annee,
        mois,
        code_commune,
        code_departement,
        nullif(type_logement, '')   as type_logement,
        nb_logements_autorises,
        nb_logements_commences,
        surface_autorisee,
        surface_commencee

    from source
    where code_commune != ''

)

select * from cleaned
