"""
DAG with a single PythonOperator that sleeps for 10 minutes in the worker.
"""

from __future__ import annotations

import time
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def sleep_ten_minutes() -> str:
    """Sleep for 10 minutes on the worker."""
    time.sleep(10 * 60)
    return "Done sleeping for 10 minutes"


with DAG(
    dag_id="sleep_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["sleep", "worker"],
) as dag:
    PythonOperator(
        task_id="sleep_ten_minutes",
        python_callable=sleep_ten_minutes,
    )
