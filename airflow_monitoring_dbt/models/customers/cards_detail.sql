{{ config(
    materialized = "incremental",
    incremental_strategy = "insert_overwrite",
    partition_by = {"field": "business_effective_date", "data_type": "date"},
) }}

with src as (
    select
        cast(business_effective_date as date) as business_effective_date,
        card_id,
        customer_id,
        account_id,
        card_type,
        cast(issued_date as date) as issued_date,
        status
    from {{ source('lz', 'cards') }}
    where cast(business_effective_date as date) = cast('{{ var("business_date") }}' as date)
)

select * from src
