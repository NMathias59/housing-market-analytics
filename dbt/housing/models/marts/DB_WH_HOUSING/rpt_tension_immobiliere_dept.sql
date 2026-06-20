{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(annee, code_departement)',
        tags         = ['rpt']
    )
}}

-- Indicateurs de tension immobilière par département et année.
-- Croise l'offre (construction, stock neuf, logement social) et la demande
-- (transactions, réservations, prix).
-- Grain : (annee, code_departement).

with dept as (

    select * from {{ ref('dim_departement') }}

),

transactions as (

    select
        annee,
        code_departement,
        count()                                         as nb_ventes,
        countIf(type_local = 'Appartement')             as nb_ventes_appt,
        countIf(type_local = 'Maison')                  as nb_ventes_maison,
        round(avg(prix_m2), 0)                          as prix_m2_moy,
        round(quantile(0.5)(prix_m2), 0)                as prix_m2_med,
        round(avg(surface_reelle_bati), 1)              as surface_moy
    from {{ ref('fct_transactions') }}
    group by annee, code_departement

),

neuf as (

    select
        annee,
        code_departement,
        prix_m2_neuf_moy,
        stock_moy                                       as stock_neuf_moy,
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
        sum(nb_autorises)                               as nb_autorises,
        sum(nb_commences)                               as nb_commences
    from {{ ref('fct_construction') }}
    group by annee, code_departement

),

social as (

    select
        annee,
        code_departement,
        sum(nb_entrees_ls)                              as nb_entrees_ls,
        round(avg(pct_passoires_ls), 1)                 as pct_passoires_ls
    from {{ ref('fct_logement_social') }}
    group by annee, code_departement

)

select
    t.annee                                         as annee,
    t.code_departement                              as code_departement,
    dept.libelle_departement,

    -- Demande : marché de l'ancien
    t.nb_ventes,
    t.nb_ventes_appt,
    t.nb_ventes_maison,
    t.prix_m2_moy,
    t.prix_m2_med,
    t.surface_moy,

    -- Offre : marché du neuf
    n.nb_mises_en_vente,
    n.nb_reservations,
    n.taux_reservation,
    n.stock_neuf_moy,
    n.delai_ecoulement_moy,
    n.prix_m2_neuf_moy,

    -- Offre : pipeline construction
    c.nb_autorises,
    c.nb_commences,

    -- Offre : logement social
    s.nb_entrees_ls,
    s.pct_passoires_ls

from transactions t
left join dept      on dept.code_departement = t.code_departement
left join neuf n    on n.code_departement = t.code_departement and n.annee = t.annee
left join construction c on c.code_departement = t.code_departement and c.annee = t.annee
left join social s  on s.code_departement = t.code_departement and s.annee = t.annee
