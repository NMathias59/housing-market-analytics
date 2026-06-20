{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(annee, code_departement, code_commune)',
        tags         = ['marts', 'dpe']
    )
}}

/*
  Agrégation DPE par commune et année.
  Utilisé pour enrichir les transactions DVF avec la performance
  énergétique moyenne du parc immobilier local.
*/

with dpe as (

    select * from {{ ref('stg_dpe') }}
    where etiquette_dpe is not null
      and surface_habitable_logement > 0

),

final as (

    select
        annee,
        code_commune,
        code_departement,

        -- Distribution des étiquettes
        countIf(etiquette_dpe = 'A')    as nb_dpe_a,
        countIf(etiquette_dpe = 'B')    as nb_dpe_b,
        countIf(etiquette_dpe = 'C')    as nb_dpe_c,
        countIf(etiquette_dpe = 'D')    as nb_dpe_d,
        countIf(etiquette_dpe = 'E')    as nb_dpe_e,
        countIf(etiquette_dpe = 'F')    as nb_dpe_f,
        countIf(etiquette_dpe = 'G')    as nb_dpe_g,
        count()                         as nb_dpe_total,

        -- Passoires thermiques (F+G)
        round(
            100.0 * countIf(etiquette_dpe in ('F', 'G')) / count(),
            1
        )                               as pct_passoires_thermiques,

        -- Conso moyenne
        round(avg(consommation_energie_primaire), 1)    as conso_ep_moy,
        round(avg(consommation_energie_finale), 1)      as conso_ef_moy,
        round(avg(emission_ges), 1)                     as emission_ges_moy,
        round(avg(surface_habitable_logement), 1)       as surface_moy

    from dpe
    group by annee, code_commune, code_departement

)

select * from final
