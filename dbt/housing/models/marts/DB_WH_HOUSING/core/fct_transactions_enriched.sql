{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(annee, code_departement, code_commune, date_mutation)',
        tags         = ['marts', 'ml']
    )
}}

-- Table ML-ready : transactions DVF enrichies des contextes communaux et départementaux.
-- Grain : un lot par ligne (même granularité que fct_transactions).
-- Toutes les jointures sont LEFT pour préserver l'intégralité des transactions.
--
-- Sources d'enrichissement :
--   fct_dpe_commune     → performance énergétique du parc local (commune × annee)
--   int_rpls_commune    → logements sociaux (commune × annee, disponible 2025 uniquement)
--   int_sitadel_commune → dynamisme construction neuve (commune × annee)
--   int_ecln_dept_annee → marché du neuf et prix de référence (département × annee)

with transactions as (

    select * from {{ ref('fct_transactions') }}

),

dpe_commune as (

    select
        annee,
        code_commune,
        nb_dpe_total,
        pct_passoires_thermiques,
        conso_ep_moy,
        conso_ef_moy,
        emission_ges_moy
    from {{ ref('fct_dpe_commune') }}

),

rpls_commune as (

    select
        annee,
        code_commune,
        nb_entrees_ls,
        nb_plai,
        nb_plus,
        nb_pls,
        pct_passoires_ls,
        surface_moy_ls,
        nb_pieces_moy_ls
    from {{ ref('int_rpls_commune') }}

),

sitadel_commune as (

    select
        annee,
        code_commune,
        nb_autorises,
        nb_commences,
        taux_concretisation
    from {{ ref('int_sitadel_commune') }}

),

ecln_dept as (

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

final as (

    select
        -- Identifiants
        t.id_mutation,
        t.id_parcelle,
        t.date_mutation                         as date_mutation,
        t.annee                                 as annee,
        t.nature_mutation,

        -- Géographie
        t.code_commune                          as code_commune,
        t.nom_commune,
        t.code_departement                      as code_departement,
        t.code_postal,
        t.adresse_nom_voie,
        t.latitude,
        t.longitude,

        -- Caractéristiques structurelles du bien
        t.type_local,
        t.surface_reelle_bati,
        t.nombre_pieces_principales,
        t.surface_terrain,
        t.valeur_fonciere,

        -- Variable cible ML
        t.prix_m2,

        -- Features DPE communales (performance énergétique du parc local)
        d.nb_dpe_total,
        d.pct_passoires_thermiques,
        d.conso_ep_moy,
        d.conso_ef_moy,
        d.emission_ges_moy,

        -- Features logement social (tension sociale et parc HLM local)
        r.nb_entrees_ls,
        r.nb_plai,
        r.nb_plus,
        r.nb_pls,
        r.pct_passoires_ls,
        r.surface_moy_ls,
        r.nb_pieces_moy_ls,

        -- Features construction (dynamisme du marché local)
        s.nb_autorises,
        s.nb_commences,
        s.taux_concretisation,

        -- Features marché du neuf départemental (prix de référence, tension offre/demande)
        e.prix_m2_neuf_moy,
        e.stock_moy,
        e.delai_ecoulement_moy,
        e.taux_reservation,
        e.nb_mises_en_vente,
        e.nb_reservations

    from transactions t
    left join dpe_commune d
        on t.code_commune = d.code_commune
        and t.annee = d.annee
    left join rpls_commune r
        on t.code_commune = r.code_commune
        and t.annee = r.annee
    left join sitadel_commune s
        on t.code_commune = s.code_commune
        and t.annee = s.annee
    left join ecln_dept e
        on t.code_departement = e.code_departement
        and t.annee = e.annee

)

select * from final
