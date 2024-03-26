from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    "owner": "Airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    default_args=default_args,
    dag_id="postgres_operator_example",
    start_date=datetime(2023, 12, 31),
    schedule_interval="@once",
) as dag:
    logging.info("INFO: Working on Postgress DAGs.")
    task_create_table = SQLExecuteQueryOperator(
        task_id="1-task-create-table",
        sql="""
        CREATE TABLE IF NOT EXISTS dag_runs(
            dt date,
            dag_id character varying,
            primary key(dt, dag_id)
        ) 
        """,
        conn_id="postgres_docker_local",
    )

    task_delete_rows = SQLExecuteQueryOperator(
        task_id="2-task-delete-data",
        sql="""
            DELETE FROM dag_runs WHERE dt =  '{{ ds }}' AND dag_id = '{{dag.dag_id}}';
        """,
        conn_id="postgres_docker_local",
    )

    tasK_insert_data = SQLExecuteQueryOperator(
        task_id="3-task-insert-data",
        sql="""
            INSERT INTO dag_runs(dt, dag_id) VALUES ('{{ ds }}', '{{dag.dag_id}}');
        """,
        conn_id="postgres_docker_local",
    )

    task_create_table >> task_delete_rows >> tasK_insert_data
