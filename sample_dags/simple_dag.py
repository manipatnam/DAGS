"""
## Simple Data Processing DAG

This is a simple DAG that demonstrates basic task dependencies using Airflow's
TaskFlow API. It performs three sequential tasks:
1. Generate some sample data
2. Process the data
3. Print the results

This DAG runs daily and shows how tasks can pass data between each other
using the TaskFlow API's automatic XCom handling.
"""

from airflow.sdk import dag, task
from pendulum import datetime
from datetime import timedelta


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Data Team", "retries": 1},
    tags=["simple", "example"],
)
def simple_data_processing():
    """A simple DAG demonstrating basic task dependencies."""

    @task
    def generate_data() -> list[int]:
        """
        Generate a list of sample numbers.
        Returns a list of integers.
        """
        data = [1, 2, 3, 4, 5]
        print(f"Generated data: {data}")
        return data

    @task
    def process_data(numbers: list[int]) -> list[int]:
        """
        Process the data by squaring each number.
        Takes a list of integers and returns a list of squared integers.
        """
        processed = [x**2 for x in numbers]
        print(f"Processed data: {processed}")
        return processed

    @task
    def print_results(results: list[int]) -> None:
        """
        Print the final results.
        Takes a list of processed numbers and prints them.
        """
        print(f"Final results: {results}")
        print(f"Sum of results: {sum(results)}")

    # Define task dependencies
    data = generate_data()
    processed = process_data(data)
    print_results(processed)


# Instantiate the DAG
simple_data_processing()

