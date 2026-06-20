{{
    config(
        materialized = 'view',
        tags = ['staging', 'ecln']
    )
}}

with source as (

    select * from {{ source('raw', 'raw_ecln') }} final

),

cleaned as (

    select
        annee,
        trimestre,
        code_departement,
        nullif(libelle_departement, '')     as libelle_departement,
        nullif(type_logement, '')           as type_logement,
        nb_mises_en_vente,
        nb_reservations,
        nb_annulations,
        stock,
        delai_ecoulement,
        prix_m2,
        prix_moyen_individuel

    from source
    where code_departement != ''

)

select * from cleaned
