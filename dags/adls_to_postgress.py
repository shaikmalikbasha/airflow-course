import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.data_lake import (
    AzureDataLakeStorageV2Hook,
)

from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago

# Define DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
}


def download_file_from_directory(directory_client, local_path: str, file_name: str):
    logging.info("Downloading file from ADLS started...")
    file_client = directory_client.get_file_client(file_name)

    with open(file=local_path, mode="wb") as local_file:
        download = file_client.download_file()
        local_file.write(download.readall())
        local_file.close()


def get_downloaded_data(local_file_path):
    return pd.read_csv(local_file_path)


def dump_data():
    pass


# Define function to process file and load into PostgreSQL
def process_and_load_file():
    container_name = "tmp"
    directory_name = "/2024/03/23/"
    file_name = "customers-100.csv"
    local_file_path = "/opt/airflow/dags/files/local-customers-100.csv"

    # Connect to Azure Data Lake
    adls_gen2_hook = AzureDataLakeStorageV2Hook(adls_conn_id="adls-v2-id")
    logging.info(
        f"Files: {adls_gen2_hook.list_files_directory(container_name, directory_name)}"
    )

    # Download file from Azure Data Lake
    directory_client = adls_gen2_hook.get_directory_client(
        file_system_name=container_name, directory_name=directory_name
    )
    download_file_from_directory(directory_client, local_file_path, file_name)
    df = get_downloaded_data(local_file_path)
    df.info()

    # # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="postgres_docker_local")
    logging.info(f"pg_hook: {pg_hook}")

    # Load DataFrame into PostgreSQL table
    pg_hook.insert_rows(table="customers", rows=df.values.tolist())


# Define DAG
dag = DAG(
    "azure_datalake_to_postgres",
    default_args=default_args,
    description="DAG to process file from Azure Data Lake and load into PostgreSQL",
    schedule_interval="@once",
)

# Define task to process and load file
process_and_load_task = PythonOperator(
    task_id="process_and_load_file",
    python_callable=process_and_load_file,
    dag=dag,
)

# Define task dependencies
process_and_load_task
