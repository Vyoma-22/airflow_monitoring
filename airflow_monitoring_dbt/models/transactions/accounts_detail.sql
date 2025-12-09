{{ config(
    materialized = "incremental",
    incremental_strategy = "insert_overwrite",
    partition_by = {"field": "business_effective_date", "data_type": "date"},
) }}

with src as (
    select
        cast(business_effective_date as date) as business_effective_date,
        account_id,
        customer_id,
        account_type,
        cast(open_date as date) as open_date,
        status,
        branch_id,
        cast(current_balance as numeric) as current_balance
    from {{ ref('accounts') }}
    where cast(business_effective_date as date) = cast('{{ var("business_date") }}' as date)
)

select * from src