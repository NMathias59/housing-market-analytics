{{
    config(
        materialized = 'view',
        tags = ['staging', 'eptb']
    )
}}

with source as (

    select * from {{ source('raw', 'raw_eptb') }} final

),

cleaned as (

    select
        annee,
        nullif(zone_code, '')       as zone_code,
        nullif(zone_libelle, '')    as zone_libelle,
        nb_terrains,
        prix_terrain_m2_moy,
        prix_terrain_m2_med,
        surface_terrain_moy,
        prix_terrain_total,
        nb_maisons,
        prix_maison_m2_moy,
        prix_maison_m2_med,
        surface_maison_moy,
        prix_maison_total

    from source

)

select * from cleaned
