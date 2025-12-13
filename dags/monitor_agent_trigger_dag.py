from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_workflows import CloudWorkflowExecuteOperator
from airflow.models.variable import Variable

GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION")

# Airflow Connection: Uses the 'google_cloud_default' connection

with DAG(
    dag_id="agent_monitor_trigger_dag",
    schedule="15 16 * * *", # Daily at 4:15 PM UTC (Post-run check time)
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["agent", "monitoring"],
) as dag:
    
    trigger_agent = CloudWorkflowExecuteOperator(
        task_id="trigger_self_healing_agent_workflow",
        workflow_id="self-healing-agent-workflow", 
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,      
        input_data='{"dag_ids": ["daily_data_ingestion_dag", "daily_metrics_dag"]}', # DAGs to monitor
        gcp_conn_id='google_cloud_default',
        deferrable=False
    )