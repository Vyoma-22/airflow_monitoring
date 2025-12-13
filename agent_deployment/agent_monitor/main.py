import functions_framework
import requests
import json
import os
from google.auth import default
from google.auth.transport.requests import AuthorizedSession
from google.cloud import logging_v2 as logging
from google.cloud import aiplatform 
from google.cloud import storage 
from urllib.parse import urljoin
from typing import List, Dict, Any

# --- Configuration (Pulled from Environment Variables) ---
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id") 
REGION = os.environ.get("GCP_REGION", "us-central1")
AIRFLOW_WEBSERVER_URL = os.environ.get("AIRFLOW_WEBSERVER_URL")
IAP_CLIENT_ID = os.environ.get("IAP_CLIENT_ID") 
COMPOSER_BUCKET_NAME = os.environ.get("COMPOSER_BUCKET").replace('gs://', '').strip('/') # e.g., my-composer-bucket-id
TEAMS_WEBHOOK_URL = os.environ.get("TEAMS_WEBHOOK_URL") # NEW: Webhook for notifications

# GCS Paths for RAG
MANIFEST_GCS_PATH = "dbt_artifacts/manifest.json"
MAPPING_GCS_PATH = "rag_data/consumer_mapping.json"

# Initialize Clients
storage_client = storage.Client()
aiplatform.init(project=PROJECT_ID, location=REGION)
gemini_model = aiplatform.get_client().get_model('gemini-2.5-flash')
logging_client = logging.LoggingServiceV2Client()


# --- Tool 1: Airflow API Authentication ---
def get_airflow_auth_session(iap_client_id: str) -> AuthorizedSession:
    """Gets an authenticated session using the Service Account and IAP Client ID."""
    scopes = ['https://www.googleapis.com/auth/cloud-platform']
    credentials, _ = default()
    return AuthorizedSession(
        credentials.with_scopes(scopes), 
        target_audience=iap_client_id
    )

# --- Tool 2: Lineage & Consumer Mapping (Real RAG Implementation) ---
def get_consumer_info(failed_model: str) -> tuple[str, List[str]]:
    """Retrieves lineage from manifest.json and consumer map from GCS to find affected teams."""
    
    bucket = storage_client.bucket(COMPOSER_BUCKET_NAME)
    
    # 1. Retrieve Lineage (dbt manifest)
    try:
        manifest_blob = bucket.blob(MANIFEST_GCS_PATH)
        manifest_data = json.loads(manifest_blob.download_as_text())
        child_map = manifest_data.get('child_map', {}) 
    except Exception as e:
        print(f"Error retrieving dbt manifest for RAG: {e}")
        child_map = {}

    # 2. Retrieve Consumer Mapping (Business RAG)
    try:
        mapping_blob = bucket.blob(MAPPING_GCS_PATH)
        consumer_mapping_list = json.loads(mapping_blob.download_as_text())
        consumer_map = {item['model']: item['consumers'] for item in consumer_mapping_list}
    except Exception as e:
        print(f"Error retrieving consumer mapping for RAG: {e}")
        consumer_map = {}

    # 3. Trace Downstream Consumers
    
    # Start tracing from the failed model
    nodes_to_check = {f"model.airflow_monitoring_dbt.{failed_model}"}
    affected_teams = set()
    
    while nodes_to_check:
        current_node_unique_id = nodes_to_check.pop()
        
        # a. Map the model back to the team
        simple_name = current_node_unique_id.split('.')[-1]
        if simple_name in consumer_map:
            affected_teams.update(consumer_map.get(simple_name, []))

        # b. Add immediate children (downstream nodes) to the list to check
        if current_node_unique_id in child_map:
            for child_id in child_map[current_node_unique_id]:
                nodes_to_check.add(child_id)

    return failed_model, list(affected_teams)

# --- Tool 3: Airflow API Operations ---
def get_failed_tasks(dag_id: str) -> List[Dict[str, Any]]:
    """Fetches details of all failed tasks for a given DAG."""
    # ... (Implementation uses authenticated session to hit /api/v1/dags/{dag_id}/dagRuns)
    # This function is complex and is assumed to return a list like:
    # [{"task_id": "run_model_customer_accounts", "dag_run_id": "scheduled_2025-..."}]
    return [] # Mock for brevity

def clear_failed_tasks(failed_tasks: List[Dict[str, Any]]):
    """Clears failed task instances to enable DAG retry."""
    # ... (Implementation uses authenticated session to hit /api/v1/dags/{dag_id}/clear)
    print(f"Clearing {len(failed_tasks)} failed tasks.")
    return len(failed_tasks)

# --- Tool 4: LLM Root Cause Analysis and Communication Draft ---
def perform_root_cause_analysis(failed_tasks: List[Dict[str, Any]]) -> str:
    """Uses Gemini to perform RCA based on task logs and generate a communication."""
    
    # 1. Gather Log Snippets and RAG Context
    log_snippets = []
    rag_context = ""
    for task in failed_tasks:
        # A real implementation would fetch the relevant log snippets from Cloud Logging
        log_snippets.append(f"Task {task['task_id']} failed at time X with error message Y...")
        
        # Use RAG for context
        failed_model_name = task['task_id'].replace("run_model_", "")
        model, consumers = get_consumer_info(failed_model_name)
        rag_context += f"- Model: {model}. Affected Consumers: {', '.join(consumers)}\n"
        
    log_data = "\n".join(log_snippets)

    # 2. Gemini Prompt
    prompt = f"""
    You are an expert Data Engineer. Analyze the following failure logs and RAG context.
    
    1. Identify the single root cause (e.g., source data schema change, BigQuery API timeout, dbt compilation error).
    2. Draft a professional, empathetic communication message to the affected consumers.
    3. The message must clearly state the failure, the root cause (briefly), and the teams affected by the downstream data delay.

    --- LOGS ---
    {log_data}
    
    --- RAG CONTEXT ---
    {rag_context}
    
    Start your response directly with the communication message (Email body/Chat message). Do not include any headers like 'Subject:' or 'To:'.
    """
    
    response = gemini_model.generate_content(prompt)
    return response.text.strip()

# --- Tool 5: Notification ---
def send_notification(rca_report: str) -> bool:
    """Sends the LLM-generated RCA report via a configured Webhook."""
    
    if not TEAMS_WEBHOOK_URL:
        print("ERROR: Teams webhook URL not configured in ENV. Skipping notification.")
        return False
    
    # Format the message for a Teams card (simplified payload example)
    message_payload = {
        "text": f"**Data Pipeline FAILURE - Critical Alert**\n\n{rca_report}"
    }

    try:
        response = requests.post(
            TEAMS_WEBHOOK_URL,
            headers={'Content-Type': 'application/json'},
            data=json.dumps(message_payload)
        )
        response.raise_for_status()
        print(f"Notification sent successfully: {response.status_code}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Notification failed: {e}")
        return False


# --- Main Entry Point ---
@functions_framework.http
def agent_executor(request):
    """
    The main entry point for the Cloud Function, triggered by the Cloud Workflow.
    """
    request_json = request.get_json(silent=True)
    
    dag_ids = request_json.get("dag_ids", [])
    action = request_json.get("action") 
    retry_count = request_json.get("retry_count", 1) # Not strictly used, but good for context

    total_failed_tasks = 0
    all_failed_tasks = []

    for dag_id in dag_ids:
        failed_tasks = get_failed_tasks(dag_id)
        all_failed_tasks.extend(failed_tasks)
        total_failed_tasks += len(failed_tasks)
        
    # --- Action Execution ---
    if action == "clear_and_retry" and total_failed_tasks > 0:
        tasks_cleared = clear_failed_tasks(all_failed_tasks)
        
        return {
            "status": "Retry Attempted",
            "failed_tasks_count": total_failed_tasks - tasks_cleared, # Should be 0 if all cleared
            "message": f"Cleared {tasks_cleared} tasks for retry attempt #{retry_count}."
        }

    elif action == "final_failure_rca" and total_failed_tasks > 0:
        # 1. Perform RCA (Uses Gemini and RAG context)
        rca_report = perform_root_cause_analysis(all_failed_tasks)
        
        # 2. Send the notification (Tool 5)
        notification_result = send_notification(rca_report)
        
        return {
            "status": "Final Notification Sent" if notification_result else "Notification Failed",
            "rca_report": rca_report
        }

    return {
        "status": "Success/No Action Needed",
        "failed_tasks_count": 0,
        "message": "No failed tasks found, or action was irrelevant."
    }