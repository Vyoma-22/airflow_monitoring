{{ config(
    materialized = "incremental",
    incremental_strategy = "insert_overwrite",
    partition_by = {"field": "business_effective_date", "data_type": "date"},
) }}

select
    a.business_effective_date,
    a.account_id,
    a.customer_id,
    c.first_name,
    c.last_name,
    a.account_type,
    a.current_balance,
    a.branch_id
from {{ ref('accounts_detail') }} a
left join {{ ref('customers_detail') }} c
  on a.customer_id = c.customer_id and a.business_effective_date = c.business_effective_date
where cast(business_effective_date as date) = cast('{{ var("business_date") }}' as date)
