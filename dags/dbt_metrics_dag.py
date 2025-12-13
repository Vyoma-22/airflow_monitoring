from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.datasets import Dataset
# KubernetesPodOperator is needed here as well

# ... (Insert get_dbt_kpo_task and definitions from dbt_daily_dag.py here) ...

# 1. Datasets (Input)
CUSTOMERS_DATASET = Dataset("s3://airflow_monitoring_dbt/datasets/customers_data")
TRANSACTIONS_DATASET = Dataset("s3://airflow_monitoring_dbt/datasets/transactions_data")
# 2. Datasets (Output)
DAILY_BALANCES_DATASET = Dataset("s3://airflow_monitoring_dbt/datasets/daily_balances") 

with DAG(
    dag_id="daily_metrics_dag",
    # Scheduled only when the upstream datasets are produced
    schedule=[CUSTOMERS_DATASET, TRANSACTIONS_DATASET], 
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "metrics", "kpo"],
) as dag:
    
    customer_accounts = get_dbt_kpo_task(
        task_id="run_model_customer_accounts",
        model_name="customer_accounts",
    )

    daily_balances = get_dbt_kpo_task(
        task_id="run_model_daily_balances",
        model_name="daily_balances",
        dataset_output=DAILY_BALANCES_DATASET 
    )

    customer_accounts >> daily_balances