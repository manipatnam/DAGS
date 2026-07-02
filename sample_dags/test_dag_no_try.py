"""
Dummy DAG that intentionally reads XCom during DAG parse, without try/except.

This is intentionally unsafe test code to reproduce parse-time behavior.
"""

from __future__ import annotations

from airflow.sdk import dag, task
from airflow.models.xcom import XCom
from airflow.utils.session import create_session
from pendulum import datetime


DAG_ID = "test_dag_no_try"
PARSE_TIME_TASK_ID = "produce_xcom_no_try"
PARSE_TIME_XCOM_KEY = "dummy_key_no_try"


def _read_xcom_during_parse_no_try() -> object:
    with create_session() as session:
        row = (
            session.query(XCom)
            .filter(
                XCom.dag_id == DAG_ID,
                XCom.task_id == PARSE_TIME_TASK_ID,
                XCom.key == PARSE_TIME_XCOM_KEY,
            )
            .order_by(XCom.id.desc())
            .first()
        )
        if not row:
            return None
        return getattr(row, "value", None)


# Evaluated at import/parse time with no exception handling.
PARSE_TIME_XCOM_VALUE = _read_xcom_during_parse_no_try()


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "xcom", "parse-time", "no-try"],
)
def test_dag_no_try():
    @task(task_id=PARSE_TIME_TASK_ID)
    def produce_xcom_no_try(**context) -> str:
        context["ti"].xcom_push(key=PARSE_TIME_XCOM_KEY, value="value_from_runtime_no_try")
        return "ignored_by_design"

    @task
    def consume_xcom_no_try(parsed_value: object) -> None:
        print(f"[no-try] XCom captured during DAG parse: {parsed_value!r}")

    produce_xcom_no_try()
    consume_xcom_no_try(PARSE_TIME_XCOM_VALUE)


test_dag_no_try()

