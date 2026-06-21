{{
    config(
        schema       = 'db_ai_house',
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(code_departement, code_commune)',
        tags         = ['ai', 'shap']
    )
}}

-- Dataset SHAP cross-sectionnel : une ligne par commune, moyennée sur 2021-2025.
-- Grain      : commune (pas de dimension temporelle — évite que les lags dominent SHAP).
-- Cible      : prix_m2_moy (prix moyen sur la période).
-- Features   : structurelles uniquement — géographie, type de bien, DPE,
--              construction, logement social, marché du neuf.
-- Résultat   : SHAP explique POURQUOI une commune est chère ou pas,
--              pas juste "parce qu'elle était déjà chère".

with base as (

    select * from {{ ref('fct_transactions_enriched') }}
    where prix_m2 > 500
      and prix_m2 < 30000

),

commune as (

    select
        code_commune,
        code_departement,

        -- Cible : prix médian sur la période (robuste aux outliers)
        round(median(prix_m2),    0)                                            as prix_m2_med,
        round(avg(prix_m2),       0)                                            as prix_m2_moy,
        round(quantile(0.25)(prix_m2), 0)                                       as prix_m2_q25,
        round(quantile(0.75)(prix_m2), 0)                                       as prix_m2_q75,
        count()                                                                 as nb_transactions_total,

        -- Type de bien — proxy urbanité (% appart élevé = commune urbaine)
        round(countIf(type_local = 'Appartement') * 100.0 / count(), 1)        as pct_appt,
        round(countIf(type_local = 'Maison')      * 100.0 / count(), 1)        as pct_maison,

        -- Caractéristiques physiques moyennes des biens vendus
        round(avg(surface_reelle_bati),       1)                                as surface_moy,
        round(avg(nombre_pieces_principales), 1)                                as nb_pieces_moy,

        -- Liquidité du marché (transactions par mois en moyenne)
        round(count() / 60.0, 1)                                                as transactions_par_mois,

        -- DPE — qualité énergétique du parc local
        any(nb_dpe_total)                                                       as nb_dpe_total,
        any(pct_passoires_thermiques)                                           as pct_passoires_thermiques,
        any(conso_ep_moy)                                                       as conso_ep_moy,
        any(conso_ef_moy)                                                       as conso_ef_moy,
        any(emission_ges_moy)                                                   as emission_ges_moy,

        -- Construction — dynamisme de l'offre neuve
        any(nb_autorises)                                                       as nb_autorises,
        any(nb_commences)                                                       as nb_commences,
        any(taux_concretisation)                                                as taux_concretisation,

        -- Logement social — structure sociale du parc
        any(nb_entrees_ls)                                                      as nb_logements_sociaux,
        any(pct_passoires_ls)                                                   as pct_passoires_ls,
        any(surface_moy_ls)                                                     as surface_moy_ls,

        -- Marché du neuf départemental — tension offre/demande du territoire
        any(prix_m2_neuf_moy)                                                   as prix_m2_neuf_dept,
        any(stock_moy)                                                          as stock_neuf_dept,
        any(delai_ecoulement_moy)                                               as delai_ecoulement_dept,
        any(taux_reservation)                                                   as taux_reservation_dept,
        any(nb_mises_en_vente)                                                  as nb_mises_en_vente_dept,

        -- Période couverte (qualité de l'observation)
        min(annee)                                                              as annee_min,
        max(annee)                                                              as annee_max

    from base
    group by code_commune, code_departement

)

-- Filtre qualité : communes avec assez de transactions pour un prix fiable
select *
from commune
where nb_transactions_total >= 20
