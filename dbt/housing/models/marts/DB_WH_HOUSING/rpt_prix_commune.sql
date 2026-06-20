{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(annee, code_departement, code_commune)',
        tags         = ['marts', 'reporting']
    )
}}

-- Statistiques de prix par commune, année et type de bien.
-- Grain : (annee, code_commune, type_local).
-- Sert à la visualisation et au benchmark des prix de marché.

with transactions as (

    select * from {{ ref('fct_transactions') }}

),

final as (

    select
        annee,
        code_commune,
        nom_commune,
        code_departement,
        type_local,

        count()                                     as nb_transactions,
        round(avg(prix_m2), 0)                      as prix_m2_moyen,
        round(quantile(0.5)(prix_m2), 0)            as prix_m2_median,
        round(quantile(0.25)(prix_m2), 0)           as prix_m2_q1,
        round(quantile(0.75)(prix_m2), 0)           as prix_m2_q3,
        round(min(prix_m2), 0)                      as prix_m2_min,
        round(max(prix_m2), 0)                      as prix_m2_max,

        round(avg(valeur_fonciere), 0)              as prix_moyen,
        round(avg(surface_reelle_bati), 1)          as surface_moy,
        round(avg(nombre_pieces_principales), 1)    as nb_pieces_moy

    from transactions
    group by annee, code_commune, nom_commune, code_departement, type_local

)

select * from final
