from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk.definitions.deadline import AsyncCallback, DeadlineAlert, DeadlineReference
from airflow.utils.email import send_email

TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

def _email_on_retry(context):
    ti = context["ti"]
    subject = f"[Retry] {ti.dag_id}.{ti.task_id} try {ti.try_number}/{ti.max_tries + 1}"
    html = f"""
    DAG: {ti.dag_id}<br>
    Task: {ti.task_id}<br>
    Try: {ti.try_number}/{ti.max_tries + 1}
    Run: {context['dag_run'].run_id} <br>
    Log: <a href="{ti.log_url}">link</a> <br>
    """
    send_email(to=["hogmail@gmail.com"], subject=subject, html_content=html)

def _email_on_final_failure(context):
    ti = context["ti"]
    is_final = ti.try_number >= (ti.max_tries + 1)
    
    if not is_final:
        return

    subject = f"[Failed] {ti.dag_id}.{ti.task_id}"
    html = f"""
    DAG: {ti.dag_id}<br>
    Task: {ti.task_id} <br>
    Run: {context["dag_run"].run_id}<br>
    Log: <a href="{ti.log_url}">link</a><br>
    """
    send_email(to=["hogmail@gmail.com"], subject=subject, html_content=html)


SLA = DeadlineAlert(
        reference = DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(minutes=2),
        callback=AsyncCallback(
            "services.common.deadline_callbacks.deadline_send_email"
        )
    )

default_args = {
    "owner": "thanh",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": True,
    "email_on_failure": True
}    

with DAG(
    dag_id="wms_pipeline",
    start_date=datetime(2026, 2 ,14, tzinfo=TZ),
    schedule="@daily",
    catchup=False,
    deadline=SLA,
    default_args=default_args
):
    
    extractor = BashOperator(
        task_id = "extractor",
        on_retry_callback=_email_on_retry,
        on_failure_callback=_email_on_final_failure,
        bash_command = (
            "cd /opt/airflow/project &&"
            "python -m services.extractor.app.run"
            "--run-id {{dag_run.run_id}}"
        )
    )
    stg_ib = BashOperator(
        task_id = "staging_ib",
        on_retry_callback=_email_on_retry,
        on_failure_callback=_email_on_final_failure,
        bash_command = (
            "cd /opt/airflow/project &&"
            "python -m services.staging.app.run "
            "--entity ib_receipts " 
            "--run-id {{dag_run.run_id}}"
        )
    )
    
    stg_ob = BashOperator(
        task_id = "staging_ob",
        on_retry_callback=_email_on_retry,
        on_failure_callback=_email_on_final_failure,        
        bash_command = (
            "cd /opt/airflow/project &&"
            "python -m services.staging.app.run "
            "--entity ob_orders " 
            "--run-id {{dag_run.run_id}}"
        )
    )
    
    mart_ib = BashOperator(
        task_id = "mart_ib",
        on_retry_callback=_email_on_retry,
        on_failure_callback=_email_on_final_failure,
        bash_command = (
            "cd /opt/airflow/project &&"
            "python -m services.mart.app.run "
            "--entity ib_receipts "
            "--run-id {{dag_run.run_id}}"
        )
    )
    
    mart_ob = BashOperator(
        task_id = "mart_ob",
        on_retry_callback=_email_on_retry,
        on_failure_callback=_email_on_final_failure,
        bash_command = (
            "cd /opt/airflow/project &&"
            "python -m services.mart.app.run "
            "--entity ob_orders "
            "--run-id {{dag_run.run_id}}"
        )
    )
        
    extractor >> [stg_ib, stg_ob]
    stg_ib >> mart_ib
    stg_ob >> mart_ob
