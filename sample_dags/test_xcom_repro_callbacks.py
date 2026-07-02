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
    dag_run = context["dag_run"]
    # In RE callback execution, dag_run can be a lightweight model that has
    # `task_instances` but not ORM methods like `get_task_instances()`.
    ti_list = getattr(dag_run, "task_instances", None)
    if ti_list is None:
        ti_getter = getattr(dag_run, "get_task_instances", None)
        ti_list = ti_getter() if callable(ti_getter) else []

    print(f"[callback] DAG succeeded: {dag_run.dag_id}")
    print(f"[callback] task instance count: {len(ti_list)}")

    for ti in ti_list:
        task_id = getattr(ti, "task_id", None)
        map_index = getattr(ti, "map_index", None)
        print(f"[callback] saw task_id={task_id!r}, map_index={map_index!r}")

        # Keep exact match first, but allow prefixed variants for debugging
        # across different runtime representations.
        if task_id == "mapped_task" or str(task_id).startswith("mapped_task"):
            try:
                val = ti.xcom_pull(task_ids="mapped_task", map_indexes=map_index)
                print(f"[callback] mapped_task[{map_index}] xcom = {val}")
            except Exception as e:
                print(f"[callback] Error reading xcom for task_id={task_id!r}: {e}")


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