"""
## Simple Data Processing DAG

This is a simple DAG that demonstrates basic task dependencies using Airflow's
TaskFlow API. It performs four sequential tasks:
1. Generate some sample data
2. Process the data
3. Calculate statistics on the processed data
4. Print the results

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
    def calculate_statistics(processed_data: list[int]) -> dict[str, float]:
        """
        Calculate statistics on the processed data.
        Takes a list of integers and returns a dictionary with statistics.
        """
        stats = {
            "max": max(processed_data),
            "min": min(processed_data),
            "average": sum(processed_data) / len(processed_data),
            "count": len(processed_data),
        }
        print(f"Statistics: {stats}")
        return stats

    @task
    def print_results(results: list[int], stats: dict[str, float]) -> None:
        """
        Print the final results and statistics.
        Takes a list of processed numbers and statistics dictionary and prints them.
        """
        print(f"Final results: {results}")
        print(f"Sum of results: {sum(results)}")
        print(f"Statistics summary:")
        print(f"  - Maximum: {stats['max']}")
        print(f"  - Minimum: {stats['min']}")
        print(f"  - Average: {stats['average']:.2f}")
        print(f"  - Count: {stats['count']}")

    # Define task dependencies
    data = generate_data()
    processed = process_data(data)
    stats = calculate_statistics(processed)
    print_results(processed, stats)


# Instantiate the DAG
simple_data_processing()

