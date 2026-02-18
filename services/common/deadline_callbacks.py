from airflow.utils.email import send_email

async def deadline_send_email(context):
    dag_run = context.get("dag_run", {})
    deadline = context.get("deadline", {})

    if isinstance(dag_run, dict):
        dag_id = dag_run.get("dag_id", "unknown_dag")
        run_id = dag_run.get("dag_run_id", "unknown_run")
    else:
        dag_id = getattr(dag_run, "dag_id", "unknown_dag")
        run_id = getattr(dag_run, "run_id", "unknown_run")

    deadline_time = deadline.get("deadline_time") if isinstance(deadline, dict) else deadline
    subject = f"[DEADLINE MISSED] {dag_id} {run_id}"
    html = f"""
    DAG: {dag_id}<br>
    Run: {run_id}<br>
    Deadline: {deadline_time}<br>
    """
    send_email(to=["hogmail@gmail.com"], subject=subject, html_content=html)
