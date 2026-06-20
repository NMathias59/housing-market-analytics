{{
    config(
        materialized = 'view',
        tags = ['staging', 'dpe']
    )
}}

with source as (

    select * from {{ source('raw', 'raw_dpe') }} final

),

cleaned as (

    select
        numero_dpe,
        date_etablissement_dpe,
        annee,
        code_commune,
        code_departement,
        nullif(etiquette_dpe, '')                       as etiquette_dpe,
        consommation_energie_primaire,
        consommation_energie_finale,
        emission_ges,
        nullif(type_energie_principale_chauffage, '')   as type_energie_principale_chauffage,
        nullif(type_installation_chauffage, '')         as type_installation_chauffage,
        surface_habitable_logement,
        nullif(periode_construction, '')                as periode_construction,
        nullif(type_batiment, '')                       as type_batiment

    from source
    where numero_dpe != ''

)

select * from cleaned
