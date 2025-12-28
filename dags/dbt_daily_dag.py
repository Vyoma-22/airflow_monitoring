from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.datasets import Dataset
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models.variable import Variable

# 1. Datasets
CUSTOMERS_DATASET = Dataset("gs://airflow_monitoring_dbt/datasets/customers_data")
TRANSACTIONS_DATASET = Dataset("gs://airflow_monitoring_dbt/datasets/transactions_data")

# 2. Configuration & Paths
DBT_IMAGE_URL = Variable.get("DBT_IMAGE_URL")
COMPOSER_BUCKET = Variable.get("COMPOSER_BUCKET").strip('/')
DBT_PROJECT_PATH = "/usr/app/airflow_monitoring_dbt" # Internal path in Docker image

def get_dbt_kpo_task(task_id: str, model_name: str, dataset_output: Dataset = None):
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace('_', '-'),
        namespace="composer-user-workloads",
        image=DBT_IMAGE_URL,
        cmds=["dbt"],
        arguments=[
            "run", 
            "--project-dir", "/usr/app/airflow_monitoring_dbt",
            "--profiles-dir", "/usr/app/airflow_monitoring_dbt",
            "--select", model_name
        ],
        env_vars={
            'GCP_PROJECT': 'cool-device-477720-k7',
            'DBT_DATASET': 'monitoring_dbt'
        },
        service_account_name="default",
    )

with DAG(
    dag_id="daily_data_ingestion_dag",
    schedule="0 14 * * *", 
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "daily", "kpo"],
) as dag:

    customers_detail = get_dbt_kpo_task(
        task_id="run_model_customers_detail", 
        model_name="customers_detail",
        dataset_output=CUSTOMERS_DATASET
    )
    
    transactions_detail = get_dbt_kpo_task(
        task_id="run_model_transactions_detail", 
        model_name="transactions_detail",
        dataset_output=TRANSACTIONS_DATASET
    )

    # Task to generate and upload the dbt manifest for the Agent
    update_artifacts = KubernetesPodOperator(
        task_id="update_dbt_artifacts",
        name="dbt-artifact-gen",
        namespace="composer-user-workloads",
        image=DBT_IMAGE_URL,
        cmds=["/bin/bash", "-c"],
        # Generates docs (which creates manifest.json) and uploads to GCS
        arguments=[
            f"dbt docs generate --project-dir {DBT_PROJECT_PATH} && gsutil cp {DBT_PROJECT_PATH}/target/manifest.json {COMPOSER_BUCKET}/dbt_artifacts/manifest.json"
        ],
        service_account_name="default", 
        trigger_rule='all_success'
    )
    
    [customers_detail, transactions_detail] >> update_artifacts