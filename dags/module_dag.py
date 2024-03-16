from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from helpers.constants import APP_NAME
from services.user_service import print_user_details

default_args = {
    "owner": "Airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="module-example-dag",
    description=f"{APP_NAME}: An example of using modules inside dags",
    schedule="@once",
    start_date=days_ago(1),
) as dag:
    task_1 = PythonOperator(
        task_id="1-get-user-details", python_callable=print_user_details
    )
    task_1
