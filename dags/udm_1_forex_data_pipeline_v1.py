import logging
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="forex-data-pipeline-v1",
    description="",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
) as dag:
    pass
