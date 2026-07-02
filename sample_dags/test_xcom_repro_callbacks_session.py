"""
Repro DAG for QA ForbiddenSessionUseError.
Callback reads mapped XCom, which may trigger get_mapped_xcom_by_slice
in the dag-processor's execution API during callback processing in RE mode.
"""
from __future__ import annotations
from datetime import datetime
from airflow.sdk import DAG, task
from airflow.models.xcom import XCom
from airflow.utils.session import create_session

def dag_success_callback(context):
    """
    Callback that reads mapped XCom from the completed DAG run.
    In RE mode, this runs inside the dag-processor's forked process,
    where session access is forbidden.
    """
    
    with create_session() as session:
        xcoms = session.query(XCom).filter(XCom.dag_id == context["dag_run"].dag_id).all()
        print(f"[callback] xcoms: {xcoms}")


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
    items = generate_items()
    mapped_results = mapped_task.expand(item=items)
    reduce_results(mapped_results)