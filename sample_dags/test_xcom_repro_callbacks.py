"""
Repro DAG for QA ForbiddenSessionUseError.
Callback reads mapped XCom, which may trigger get_mapped_xcom_by_slice
in the dag-processor's execution API during callback processing in RE mode.
"""
from __future__ import annotations
from datetime import datetime
from airflow.sdk import DAG, task


def dag_success_callback(context):
    """
    Callback that reads mapped XCom from the completed DAG run.
    In RE mode, this runs inside the dag-processor's forked process,
    where session access is forbidden.
    """
    ti= context["ti"]
    # print(f"[callback] task instance count: {len(ti_list)}")

    xcom = ti.xcom_pull(task_ids="mapped_task", map_indexes=0)
    print(f"[callback] xcom = {xcom}")


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
    dag_id="repro_qa_forbidden_session_v2",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    on_success_callback=dag_success_callback,
    tags=["repro", "qa", "mapped-xcom"],
) as dag:
    items = generate_items()
    mapped_results = mapped_task.expand(item=items)
    reduce_results(mapped_results)