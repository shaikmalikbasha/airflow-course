import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}


def catch_up_func(**kwargs):
    logging.info("Python function has been triggered...")
    logging.info(f"Magical Macros are as follows:")
    logging.info(
        f"""
            ExecutionDate: '{kwargs["ds"]}', 
            RunId: '{kwargs["run_id"]}', 
            ts: '{kwargs["ts"]}', 
            FullExecutionDate: '{kwargs["execution_date"]}'
        """
    )


with DAG(
    dag_id="start-dt-exec-dt-example",
    description="An example of start date and execution date",
    schedule="*/10 * * * *",
    start_date=datetime(2024, 3, 3, 9),
) as dag:
    task = PythonOperator(
        task_id="log-start-dt-and-exec-dt",
        python_callable=catch_up_func,
        op_kwargs={
            "ds": "{{ds}}",
            "run_id": "{{run_id}}",
            "ts": "{{ts}}",
            "execution_date": "{{execution_date}}",
        },
    )

    task
