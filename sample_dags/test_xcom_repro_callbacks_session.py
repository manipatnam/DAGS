"""
Repro DAG for QA ForbiddenSessionUseError.

A DAG-level on_success_callback reads XComs produced by the run. It uses the
Task SDK (ti.xcom_pull), which routes through the dag-processor's execution API
(GetXCom / GetXComSequenceItem / GetXComSequenceSlice) instead of a metadata-DB
session -- so it does NOT raise ForbiddenSessionUseError in RE mode.

DAG-level callbacks run inside the dag-processor's forked process; their output
lands in the dag-processor per-file log
(/usr/local/airflow/logs/dag_processor/<date>/dags/<file>.py.log), NOT in the
task logs shown in the Airflow UI.
"""
from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, task


def dag_success_callback(context):
    """Pull XComs produced by this run from inside the DAG success callback."""
    ti = context.get("ti")
    if ti is None:
        # DAG-level callbacks only get "ti" when the server includes last_ti.
        print(f"[callback] no 'ti' in context; keys={list(context)}")
        return

    # Single, non-mapped XCom -> GetXCom
    single = ti.xcom_pull(task_ids="push_value")
    print(f"[callback] push_value xcom = {single}")

    # One specific mapped index -> GetXComSequenceItem
    one_mapped = ti.xcom_pull(task_ids="mapped_task", map_indexes=0)
    print(f"[callback] mapped_task[0] xcom = {one_mapped}")

    # Whole mapped sequence -> GetXComSequenceSlice (get_mapped_xcom_by_slice)
    all_mapped = ti.xcom_pull(task_ids="mapped_task", map_indexes=None)
    print(f"[callback] mapped_task[*] xcoms = {all_mapped}")


@task
def push_value():
    return "hello-xcom"


@task
def generate_items():
    return [1, 2, 3, 4, 5]


@task
def mapped_task(item: int):
    return item * 10


@task
def reduce_results(results: list[int]):
    print(f"[reduce_results] results={results}, sum={sum(results)}")
    return sum(results)


with DAG(
    dag_id="test_xcom_repro_callbacks_session",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    on_success_callback=dag_success_callback,
    tags=["repro", "qa", "mapped-xcom"],
) as dag:
    push_value()
    items = generate_items()
    mapped_results = mapped_task.expand(item=items)
    reduce_results(mapped_results)
