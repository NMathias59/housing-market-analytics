{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(annee, code_departement)',
        tags         = ['rpt']
    )
}}

-- Dynamique globale du marché immobilier par département et année.
-- Croise : ancien (DVF), neuf (ECLN), construction (Sit@del), énergie (DPE).
-- Grain : (annee, code_departement).

with dept as (

    select * from {{ ref('dim_departement') }}

),

ancien as (

    select
        annee,
        code_departement,
        count()                                 as nb_transactions,
        round(avg(prix_m2), 0)                  as prix_m2_moyen_ancien,
        round(quantile(0.5)(prix_m2), 0)        as prix_m2_median_ancien,
        round(avg(surface_reelle_bati), 1)      as surface_moy
    from {{ ref('fct_transactions') }}
    group by annee, code_departement

),

neuf as (

    select
        annee,
        code_departement,
        prix_m2_neuf_moy,
        stock_moy,
        delai_ecoulement_moy,
        taux_reservation,
        nb_mises_en_vente,
        nb_reservations
    from {{ ref('int_ecln_dept_annee') }}

),

construction as (

    select
        annee,
        code_departement,
        sum(nb_autorises)   as nb_autorises,
        sum(nb_commences)   as nb_commences
    from {{ ref('fct_construction') }}
    group by annee, code_departement

),

-- Deux CTEs pour éviter l'alias sum(nb_dpe_total) AS nb_dpe_total
-- réutilisé dans la même expression → agrégat imbriqué ClickHouse
dpe_agg as (

    select
        annee,
        code_departement,
        sum(nb_dpe_f) + sum(nb_dpe_g)           as sum_dpe_fg,
        sum(nb_dpe_total)                        as nb_dpe_total,
        round(avg(conso_ep_moy), 1)              as conso_ep_moy,
        round(avg(emission_ges_moy), 1)          as emission_ges_moy
    from {{ ref('fct_dpe_commune') }}
    group by annee, code_departement

),

dpe as (

    select
        annee,
        code_departement,
        round(100.0 * sum_dpe_fg / nullif(nb_dpe_total, 0), 1) as pct_passoires,
        conso_ep_moy,
        emission_ges_moy,
        nb_dpe_total
    from dpe_agg

)

select
    a.annee                                     as annee,
    a.code_departement                          as code_departement,
    d.libelle_departement,

    -- Marché de l'ancien
    a.nb_transactions,
    a.prix_m2_moyen_ancien,
    a.prix_m2_median_ancien,
    a.surface_moy,

    -- Marché du neuf
    n.prix_m2_neuf_moy,
    n.nb_mises_en_vente,
    n.nb_reservations,
    n.taux_reservation,
    n.stock_moy,
    n.delai_ecoulement_moy,

    -- Ratio neuf / ancien (indicateur de surchauffe du neuf)
    round(
        n.prix_m2_neuf_moy / nullif(a.prix_m2_moyen_ancien, 0),
        2
    )                                           as ratio_prix_neuf_ancien,

    -- Pipeline construction
    c.nb_autorises,
    c.nb_commences,

    -- Qualité énergétique du parc
    dpe.pct_passoires,
    dpe.conso_ep_moy,
    dpe.emission_ges_moy,
    dpe.nb_dpe_total

from ancien a
left join dept d         on d.code_departement = a.code_departement
left join neuf n         on n.code_departement = a.code_departement and n.annee = a.annee
left join construction c on c.code_departement = a.code_departement and c.annee = a.annee
left join dpe            on dpe.code_departement = a.code_departement and dpe.annee = a.annee
