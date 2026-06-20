{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(annee, code_departement)',
        tags         = ['rpt']
    )
}}

-- Performance énergétique du parc immobilier par département et année.
-- Agrégation de fct_dpe_commune à l'échelle départementale.
-- Grain : (annee, code_departement).

with dept as (

    select * from {{ ref('dim_departement') }}

),

-- Sums d'abord — ratios dans le SELECT final pour éviter le conflit d'alias ClickHouse
-- (sum(nb_dpe_a) AS nb_dpe_a puis sum(nb_dpe_a) réutilisé = agrégat imbriqué)
dpe_dept as (

    select
        annee,
        code_departement,
        sum(nb_dpe_a)                       as nb_dpe_a,
        sum(nb_dpe_b)                       as nb_dpe_b,
        sum(nb_dpe_c)                       as nb_dpe_c,
        sum(nb_dpe_d)                       as nb_dpe_d,
        sum(nb_dpe_e)                       as nb_dpe_e,
        sum(nb_dpe_f)                       as nb_dpe_f,
        sum(nb_dpe_g)                       as nb_dpe_g,
        sum(nb_dpe_total)                   as nb_dpe_total,
        round(avg(conso_ep_moy), 1)         as conso_ep_moy,
        round(avg(conso_ef_moy), 1)         as conso_ef_moy,
        round(avg(emission_ges_moy), 1)     as emission_ges_moy,
        round(avg(surface_moy), 1)          as surface_moy
    from {{ ref('fct_dpe_commune') }}
    group by annee, code_departement

)

select
    d.annee,
    d.code_departement,
    dept.libelle_departement,
    d.nb_dpe_a,
    d.nb_dpe_b,
    d.nb_dpe_c,
    d.nb_dpe_d,
    d.nb_dpe_e,
    d.nb_dpe_f,
    d.nb_dpe_g,
    d.nb_dpe_total,
    round(100.0 * (d.nb_dpe_a + d.nb_dpe_b + d.nb_dpe_c) / nullif(d.nb_dpe_total, 0), 1) as pct_bonne_classe,
    round(100.0 * (d.nb_dpe_f + d.nb_dpe_g) / nullif(d.nb_dpe_total, 0), 1)               as pct_passoires,
    d.conso_ep_moy,
    d.conso_ef_moy,
    d.emission_ges_moy,
    d.surface_moy
from dpe_dept d
left join dept using (code_departement)
