import logging

from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook


class AzureDatalakeService:
    def __init__(self, adls_conn_id: str) -> None:
        self.adls_conn_obj = AzureDataLakeStorageV2Hook(adls_conn_id=adls_conn_id)

    def poke(self, container_name, dir_name, file_name):
        logging.info(
            f"List of Files in the Directory: {self.adls_conn_obj.list_files_directory(container_name, dir_name)}"
        )
        dir_client = self.adls_conn_obj.get_directory_client(
            file_system_name=container_name, directory_name=dir_name
        )
        return dir_client.get_file_client(file=file_name).exists()
