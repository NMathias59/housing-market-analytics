{{
    config(
        schema       = 'db_ai_house',
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(code_commune, annee, mois)',
        tags         = ['ai', 'features']
    )
}}

-- Feature table pour deep learning (LSTM / Transformer).
-- Grain      : commune × mois.
-- Source     : fct_transactions_enriched (DB_WH_HOUSING) — toutes les jointures déjà faites.
-- Cible      : prix_m2_moy.
-- Limitation : spine portée par DVF — mois sans transaction absents.

with base as (

    select * from {{ ref('fct_transactions_enriched') }}

),

commune_mois as (

    select
        annee,
        toMonth(date_mutation)                                                  as mois,
        makeDate(annee, toMonth(date_mutation), 1)                              as date_mois,
        code_commune,
        code_departement,

        -- Cible
        round(avg(prix_m2),           2)                                        as prix_m2_moy,
        round(median(prix_m2),        2)                                        as prix_m2_med,
        round(quantile(0.1)(prix_m2), 2)                                        as prix_m2_p10,
        round(quantile(0.9)(prix_m2), 2)                                        as prix_m2_p90,
        count()                                                                 as nb_transactions,

        -- Caractéristiques du parc vendu
        round(avg(surface_reelle_bati),       1)                                as surface_moy,
        round(avg(nombre_pieces_principales), 1)                                as nb_pieces_moy,
        round(countIf(type_local = 'Appartement') * 100.0 / count(), 1)        as pct_appt,
        round(countIf(type_local = 'Maison')      * 100.0 / count(), 1)        as pct_maison,

        -- DPE (annuel, même valeur sur tous les mois de l'année)
        any(nb_dpe_total)               as nb_dpe_total,
        any(pct_passoires_thermiques)   as pct_passoires_thermiques,
        any(conso_ep_moy)               as conso_ep_moy,
        any(conso_ef_moy)               as conso_ef_moy,
        any(emission_ges_moy)           as emission_ges_moy,

        -- Construction (annuel)
        any(nb_autorises)               as nb_autorises,
        any(nb_commences)               as nb_commences,
        any(taux_concretisation)        as taux_concretisation,

        -- Logement social (annuel)
        any(nb_entrees_ls)              as nb_entrees_ls,
        any(pct_passoires_ls)           as pct_passoires_ls,
        any(surface_moy_ls)             as surface_moy_ls,

        -- Marché du neuf dept (annuel)
        any(prix_m2_neuf_moy)           as prix_m2_neuf_moy,
        any(stock_moy)                  as stock_moy,
        any(delai_ecoulement_moy)       as delai_ecoulement_moy,
        any(taux_reservation)           as taux_reservation,
        any(nb_mises_en_vente)          as nb_mises_en_vente,
        any(nb_reservations)            as nb_reservations,

        -- Encodage cyclique du mois
        round(sin(2 * pi() * toMonth(date_mutation) / 12), 6)                  as mois_sin,
        round(cos(2 * pi() * toMonth(date_mutation) / 12), 6)                  as mois_cos

    from base
    where prix_m2 > 0
    group by annee, mois, code_commune, code_departement

),

with_lags as (

    select
        *,

        -- Lags temporels (mémoire explicite pour LSTM)
        lagInFrame(prix_m2_moy,  1) over w      as prix_m2_lag_1m,
        lagInFrame(prix_m2_moy,  3) over w      as prix_m2_lag_3m,
        lagInFrame(prix_m2_moy,  6) over w      as prix_m2_lag_6m,
        lagInFrame(prix_m2_moy, 12) over w      as prix_m2_lag_12m,

        -- Moyennes glissantes
        round(avg(prix_m2_moy) over (
            partition by code_commune order by annee, mois
            rows between 2 preceding and current row
        ), 2)                                   as prix_m2_roll3m,

        round(avg(prix_m2_moy) over (
            partition by code_commune order by annee, mois
            rows between 5 preceding and current row
        ), 2)                                   as prix_m2_roll6m,

        round(avg(prix_m2_moy) over (
            partition by code_commune order by annee, mois
            rows between 11 preceding and current row
        ), 2)                                   as prix_m2_roll12m,

        -- Momentum
        round(
            (prix_m2_moy - lagInFrame(prix_m2_moy,  1) over w)
            / nullif(lagInFrame(prix_m2_moy,  1) over w, 0) * 100,
        2)                                      as evol_1m_pct,

        round(
            (prix_m2_moy - lagInFrame(prix_m2_moy, 12) over w)
            / nullif(lagInFrame(prix_m2_moy, 12) over w, 0) * 100,
        2)                                      as evol_12m_pct

    from commune_mois
    window w as (partition by code_commune order by annee, mois)

)

select * from with_lags
