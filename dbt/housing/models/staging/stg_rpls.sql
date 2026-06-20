{{
    config(
        materialized = 'view',
        tags = ['staging', 'rpls']
    )
}}

with source as (

    select * from {{ source('raw', 'raw_rpls') }} final

),

cleaned as (

    select
        annee,
        code_commune,
        code_departement,
        code_region,
        nullif(financement, '')          as financement,
        nullif(classe_energie_dpe, '')   as classe_energie_dpe,
        annee_construction,
        surface,
        nb_pieces

    from source
    where code_commune != ''

)

select * from cleaned
