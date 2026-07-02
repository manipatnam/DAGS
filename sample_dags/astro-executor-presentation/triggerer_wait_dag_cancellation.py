"""
DAG with a deferrable sensor that waits 30 minutes on the triggerer only.

Unlike a PythonOperator sleep, this task defers after startup and does not
hold a worker slot for the full wait duration.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.sensors.time_delta import TimeDeltaSensorAsync
from airflow.sdk import DAG

with DAG(
    dag_id="triggerer_wait_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["wait", "triggerer", "deferrable"],
) as dag:
    TimeDeltaSensorAsync(
        task_id="wait_thirty_minutes",
        delta=timedelta(minutes=30),
    )
