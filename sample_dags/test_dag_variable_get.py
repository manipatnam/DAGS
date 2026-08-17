"""
Dummy DAG that intentionally reads a Variable during DAG parse.

This is an anti-pattern in real Airflow deployments because Variable values
should be read at task runtime, not during DAG parse (which happens frequently).
This file is only meant for testing/parsing behavior in Remote Execution.
"""

from __future__ import annotations

from airflow.sdk import Variable, dag, task
from pendulum import datetime

DAG_ID = "test_dag_variable_get"
VARIABLE_KEY = "test_parse_time_variable"
# DEFAULT_VALUE = "default_from_parse_time"

# Evaluated at import time (i.e., while the dag-processor parses this file).
PARSE_TIME_VARIABLE_VALUE = Variable.get(VARIABLE_KEY)#, default=DEFAULT_VALUE)


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "variable", "parse-time"],
)
def test_dag_variable_get():
    @task
    def consume_variable(parsed_value: str) -> None:
        print(f"Variable captured during DAG parse: {parsed_value!r}")

    @task
    def read_variable_at_runtime() -> None:
        runtime_value = Variable.get(VARIABLE_KEY, default=DEFAULT_VALUE)
        print(f"Variable read at task runtime: {runtime_value!r}")

    consume_variable(PARSE_TIME_VARIABLE_VALUE)
    read_variable_at_runtime()


test_dag_variable_get()
