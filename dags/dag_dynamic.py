import logging

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {"owner": "shaikmalikbasha"}


def create_dynamic_dag(
    dag_id,
):
    @dag(
        dag_id=dag_id,
        description="Dynamic Dag Creation Example",
        schedule_interval="@daily",
        start_date=days_ago(1),
        default_args=default_args,
    )
    def dynamic_dag_creation_example():
        @task(task_id=f"task-{dag_id}")
        def log_dummy():
            logging.info("I am dummy task...")
            logging.info(f"This is task from DAG: {dag_id}")

        log_dummy()

    generated_dag = dynamic_dag_creation_example()
    return generated_dag


for i in range(1, 3):
    dag_id = f"dynamic-dag-{i}"
    globals()[dag_id] = create_dynamic_dag(dag_id=dag_id)
