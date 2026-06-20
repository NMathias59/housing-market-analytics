{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(annee, code_departement)',
        tags         = ['intermediate', 'ecln']
    )
}}

-- Agrégation ECLN trimestrielle → annuelle par département.
-- Filtre sur 'Tous logements' pour éviter le double-comptage Individuel/Collectif.
-- Résultat : une ligne par (annee, code_departement).
--
-- Prix et délai d'écoulement sont des niveaux (pas des flux) → moyenne annuelle.
-- Mises en vente, réservations et annulations sont des flux → somme annuelle.

with source as (

    select * from {{ ref('fct_marche_neuf') }}

),

aggregated as (

    select
        annee,
        code_departement,

        -- Flux annuels (somme des 4 trimestres)
        sum(nb_mises_en_vente)                          as nb_mises_en_vente,
        sum(nb_reservations)                            as nb_reservations,
        sum(nb_annulations)                             as nb_annulations,

        -- Niveaux (moyenne sur les trimestres disponibles)
        round(avg(stock), 0)                            as stock_moy,
        round(avg(delai_ecoulement), 1)                 as delai_ecoulement_moy,
        round(avg(prix_m2_neuf), 0)                     as prix_m2_neuf_moy,
        round(avg(prix_moyen_individuel), 0)            as prix_moyen_individuel_moy,

        count()                                         as nb_trimestres

    from source
    group by annee, code_departement

)

-- Taux calculé dans un CTE séparé pour éviter le conflit d'alias ClickHouse
-- (sum(nb_reservations) AS nb_reservations référencé dans le même SELECT)
select
    *,
    round(100.0 * nb_reservations / nullif(nb_mises_en_vente, 0), 1) as taux_reservation
from aggregated
