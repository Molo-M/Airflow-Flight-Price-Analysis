"""
Alerts for the pipeline.

Airflow calls task_failure_alert automatically whenever a task fails
(after any retries), because it is set as on_failure_callback in the DAG.
For now the alert is written to the task log. To send it somewhere else
later (for example by email), only this file needs to change.
"""

import logging

log = logging.getLogger(__name__)


def task_failure_alert(context):
    """
    Write a clearly marked failure alert to the task log.

    Airflow passes `context`, a dictionary describing the failed task:
    which DAG and task it was, which run, and the error that occurred.
    """
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    error = context.get("exception")

    dag_id = task_instance.dag_id if task_instance else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    attempt = task_instance.try_number if task_instance else "unknown"
    run_id = dag_run.run_id if dag_run else "unknown"

    message = (
        "\n"
        "=================== PIPELINE FAILURE ALERT ===================\n"
        f"DAG:     {dag_id}\n"
        f"Task:    {task_id}\n"
        f"Run:     {run_id}\n"
        f"Attempt: {attempt}\n"
        f"Error:   {error}\n"
        "==============================================================="
    )
    log.error(message)
