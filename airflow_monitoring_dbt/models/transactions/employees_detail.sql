{{ config(
    materialized = "incremental",
    incremental_strategy = "insert_overwrite",
    partition_by = {"field": "business_effective_date", "data_type": "date"},
) }}

with src as (
    select
        cast(business_effective_date as date) as business_effective_date,
        employee_id,
        branch_id,
        full_name,
        role,
        cast(hire_date as date) as hire_date
    from {{ ref('employees') }}
    where cast(business_effective_date as date) = cast('{{ var("business_date") }}' as date)
)

select * from src