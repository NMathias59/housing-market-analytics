{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(annee, code_departement, date_mutation)',
        tags         = ['marts', 'dvf']
    )
}}

/*
  Fait central : transactions immobilières avec prix au m² calculé.
  Périmètre : lots avec surface > 0 et prix > 0 (ventes réelles).
  Granularité : un lot par ligne (même granularité que stg_dvf).
*/

with transactions as (

    select * from {{ ref('stg_dvf') }}
    where valeur_fonciere > 0
      and surface_reelle_bati > 0
      and type_local is not null

),

final as (

    select
        id_mutation,
        id_parcelle,
        date_mutation,
        annee,
        nature_mutation,
        type_local,
        code_commune,
        nom_commune,
        code_departement,
        code_postal,
        adresse_nom_voie,
        surface_reelle_bati,
        nombre_pieces_principales,
        surface_terrain,
        valeur_fonciere,

        -- Prix au m² calculé au niveau du lot
        round(valeur_fonciere / surface_reelle_bati, 2)     as prix_m2,

        latitude,
        longitude

    from transactions

)

select * from final
