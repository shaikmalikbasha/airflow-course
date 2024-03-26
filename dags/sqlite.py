from datetime import datetime, timedelta
from airflow import DAG

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "Airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    default_args=default_args,
    dag_id="sqlite_operator_example",
    start_date=datetime(2023, 12, 31),
    schedule_interval="@once",
) as dag:
    task_create_table = SQLExecuteQueryOperator(
        task_id="task_create_table",
        # database="test",
        sql="""
        CREATE TABLE IF NOT EXISTS dag_runs(
            ds date,
            dag_id character varying,
            primary_key(ds, dag_id)
        ) 
        """,
        conn_id="local_sqlite_id",
    )

    task_create_table
