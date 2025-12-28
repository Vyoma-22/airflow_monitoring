{{ config(
    materialized = "incremental",
    incremental_strategy = "insert_overwrite",
    partition_by = {"field": "business_effective_date", "data_type": "date"},
) }}

with src as (
    select
        cast(business_effective_date as date) as business_effective_date,
        branch_id,
        branch_name,
        city
    from {{ source('lz', 'branches') }}
    where cast(business_effective_date as date) = cast('{{ var("business_date") }}' as date)
)

select * from src
