from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}


def test_func(**kwargs):
    return f"Testing App with Arguments: Name: {kwargs['name']}"


dag = DAG(
    dag_id="new-style-of-writing",
    description="New Style",
    schedule="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
)


task_1 = PythonOperator(
    task_id="1-task", python_callable=test_func, op_kwargs={"name": "Shaik"}, dag=dag
)
task_2 = PythonOperator(
    task_id="2-task", python_callable=test_func, op_kwargs={"name": "Malik"}, dag=dag
)
task_3 = PythonOperator(
    task_id="3-task", python_callable=test_func, op_kwargs={"name": "Basha"}, dag=dag
)


task_1 >> task_2 >> task_3
