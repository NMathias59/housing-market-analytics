{{
    config(
        materialized = 'view',
        tags = ['staging', 'dvf']
    )
}}

with source as (

    -- FINAL force la déduplication ReplacingMergeTree avant lecture
    select * from {{ source('raw', 'raw_dvf') }} final

),

cleaned as (

    select
        id_mutation,
        id_parcelle,
        date_mutation,
        annee,
        nature_mutation,
        valeur_fonciere,
        adresse_numero,
        adresse_nom_voie,
        code_postal,
        code_commune,
        nom_commune,
        code_departement,
        nullif(type_local, '')                  as type_local,
        surface_reelle_bati,
        nombre_pieces_principales,
        surface_terrain,
        latitude,
        longitude

    from source
    where id_mutation != ''

)

select * from cleaned
