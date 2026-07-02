"""
Dummy DAG that intentionally reads XCom during DAG parse.

This is an anti-pattern in real Airflow deployments because XCom values are only
available after task runs and because DAG parse happens frequently. This file is
only meant for testing/parsing behavior.
"""

from __future__ import annotations

from airflow.sdk import dag, task
from pendulum import datetime

# ---------------------------------------------------------------------------
# Parse-time XCom access (intentional).
# ---------------------------------------------------------------------------

DAG_ID = "test_dag"
PARSE_TIME_TASK_ID = "produce_xcom"
PARSE_TIME_XCOM_KEY = "dummy_key"


def _read_xcom_during_parse() -> object:
    """
    Attempt to query XCom during module import (DAG parse).

    We guard this heavily so DAG loading doesn't fail even if the metadata DB
    isn't available yet (common in dev/test environments).
    """

    try:
        # Airflow imports are intentionally inside the try/except to avoid
        # hard failures during DAG parse.
        from airflow.models.xcom import XCom
        from airflow.utils.session import create_session

        with create_session() as session:
            # Order by XCom.id to avoid relying on execution_date/run_id columns
            # which vary across Airflow versions.
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
    except Exception as e:  # noqa: BLE001 - this is test/diagnostic code
        # Return a string describing the failure so the DAG can still load.
        print(f"PARSE_TIME_XCOM_READ_ERROR: {type(e).__name__}: {e}")
        return f"PARSE_TIME_XCOM_READ_ERROR: {type(e).__name__}: {e}"


# This is evaluated at import time (i.e., while the scheduler parses the DAG file).
PARSE_TIME_XCOM_VALUE = _read_xcom_during_parse()


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "xcom", "parse-time"],
)
def test_dag():
    @task(task_id=PARSE_TIME_TASK_ID)
    def produce_xcom(**context) -> str:
        # Push to a deterministic key so the parse-time lookup can find it.
        context["ti"].xcom_push(key=PARSE_TIME_XCOM_KEY, value="value_from_runtime")
        return "ignored_by_design"

    @task
    def consume_xcom(parsed_value: object) -> None:
        # `parsed_value` is captured at parse time.
        print(f"XCom captured during DAG parse: {parsed_value!r}")

    # Dependency: produce_xcom runs first, but consume_xcom uses the
    # parse-time constant (which won't update until the next DAG parse).
    produce_xcom()
    consume_xcom(PARSE_TIME_XCOM_VALUE)


test_dag()

