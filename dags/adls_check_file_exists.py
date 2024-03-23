import logging
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task


from airflow.providers.microsoft.azure.hooks.data_lake import (
    AzureDataLakeStorageV2Hook,
)
from services.adls_service import AzureDatalakeService

default_args = {"onwer": "Airflow"}

# adls_gen2_hook = AzureDataLakeStorageV2Hook(adls_conn_id="adls-v2-id")
adl_service = AzureDatalakeService(adls_conn_id="adls-v2-id")


@dag(
    dag_id="adls-gen-2-file-check-hook",
    description="DAG to check if the file exists in ADLS Gen 2",
    schedule="@once",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
)
def adls_file_check_process():
    # @task(task_id="id-check-if-file-exists")
    # def check_if_file_exists(adls_gen2_hook: AzureDataLakeStorageV2Hook):
    #     container_name = "tmp"
    #     dir_name = "2024/03/23"
    #     file_name = "customers-101.csv"
    #     # Connect to Azure Data Lake

    #     logging.info(
    #         f"Files: {adls_gen2_hook.list_files_directory(container_name, dir_name)}"
    #     )
    #     dir_client = adls_gen2_hook.get_directory_client(
    #         file_system_name=container_name, directory_name=dir_name
    #     )
    #     return dir_client.get_file_client(file=file_name).exists()
    @task(task_id="id-check-if-file-exists-2")
    def check_if_file_exists(container_name, dir_name, file_name):
        return adl_service.poke(container_name, dir_name, file_name)

    is_file_exists = check_if_file_exists("tmp", "2024/03/23", "customers-100.csv")
    # is_file_exists = check_if_file_exists(adls_gen2_hook)
    logging.info(is_file_exists)


adls_file_check_process()
