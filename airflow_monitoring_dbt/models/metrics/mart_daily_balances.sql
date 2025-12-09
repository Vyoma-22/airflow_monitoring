{{ config(
    materialized = "incremental",
    incremental_strategy = "insert_overwrite",
    partition_by = {"field": "business_effective_date", "data_type": "date"},
) }}

with txns as (
    select
        account_id,
        date(txn_timestamp) as txn_date,
        sum(case when txn_type = 'credit' then amount when txn_type = 'debit' then -amount end) as net_change
    from {{ ref('transactions_detail') }}
    where cast(business_effective_date as date) = cast('{{ var("business_date") }}' as date) 
    group by 1,2
),

balances as (
    select
        account_id,
        current_balance
    from {{ ref('accounts_detail') }}
    where cast(business_effective_date as date) = cast('{{ var("business_date") }}' as date)
),

calendar as (
    select distinct date(txn_timestamp) as date from {{ ref('transactions_detail') }} 
    where cast(business_effective_date as date) = cast('{{ var("business_date") }}' as date)
),

running_balances as (
    select
        b.account_id,
        c.date as business_effective_date,
        sum(coalesce(t.net_change,0)) over (
            partition by b.account_id
            order by c.date
        ) + b.current_balance as daily_balance
    from balances b
    cross join calendar c
    left join txns t
      on t.account_id = b.account_id
      and t.txn_date = c.date
)

select * from running_balances
