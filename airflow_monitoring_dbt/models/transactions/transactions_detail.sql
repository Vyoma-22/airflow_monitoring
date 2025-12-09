{{ config(
    materialized = "incremental",
    incremental_strategy = "insert_overwrite",
    partition_by = {"field": "business_effective_date", "data_type": "date"},
) }}


with src as (
    select
        cast(business_effective_date as date) as business_effective_date,
        txn_id,
        account_id,
        cast(txn_timestamp as timestamp) as txn_timestamp,
        txn_type,
        cast(amount as numeric) as amount,
        merchant,
        city,
        category
    from {{ ref('transactions') }}
    where cast(business_effective_date as date) = cast('{{ var("business_date") }}' as date)
)

select * from src