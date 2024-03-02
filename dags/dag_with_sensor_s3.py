from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    "owner": "shaikmalikbasha583",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="s3-minIO-sensor-dag",
    description="S3 Sensor Example",
    schedule_interval="@once",
    start_date=datetime(2024, 2, 28),
    default_args=default_args,
):
    task = S3KeySensor(
        task_id="task-sensor-minIO-s3",
        bucket_key="employees.csv",
        bucket_name="airflow",
        aws_conn_id="id-minIO-s3",
    )
    task
