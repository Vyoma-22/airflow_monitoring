{{ config(
    materialized = "incremental",
    incremental_strategy = "insert_overwrite",
    partition_by = {"field": "business_effective_date", "data_type": "date"},
) }}

with src as (
    select
        cast(business_effective_date as date) as business_effective_date,
        customer_id,
        first_name,
        last_name,
        dob,
        email,
        phone,
        cast(join_date as date) as join_date,
        city
    from {{ ref('customers') }}
    where cast(business_effective_date as date) = cast('{{ var("business_date") }}' as date)
)

select * from src
