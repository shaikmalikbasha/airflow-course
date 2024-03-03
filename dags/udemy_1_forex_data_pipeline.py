import csv
import json
import logging
from datetime import timedelta

import httpx
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=3),
}


# Download forex rates according to the currencies we want to watch
# described in the file forex_currencies.csv
def download_rates():
    logging.info("The downloading has been started...")
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        "USD": "api_forex_exchange_usd.json",
        "EUR": "api_forex_exchange_eur.json",
    }
    with open("/opt/airflow/dags/files/forex_currencies.csv") as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=";")
        for idx, row in enumerate(reader):
            base = row["base"]
            with_pairs = row["with_pairs"].split(" ")
            indata = httpx.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {"base": base, "rates": {}, "last_update": indata["date"]}
            for pair in with_pairs:
                outdata["rates"][pair] = indata["rates"][pair]
            with open("/opt/airflow/dags/files/forex_rates.json", "a") as outfile:
                json.dump(outdata, outfile)
                outfile.write("\n")


with DAG(
    dag_id="forex-data-pipeline-v1",
    description="",
    schedule="@once",
    # schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
) as dag:

    is_forex_rates_available = HttpSensor(
        task_id="is-forex-rates-available",
        endpoint="/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        http_conn_id="forex-api",
        method="GET",
        response_check=lambda res: "rates" in res.text,
        poke_interval=5,
        timeout=20,
    )

    is_forex_currencies_file_available = FileSensor(
        task_id="is-forex-currensies-file-available",
        fs_conn_id="forex-file-path",
        filepath="forex_currencies.csv",
        poke_interval=5,
        timeout=20,
    )

    download_rates_data = PythonOperator(
        task_id="download-forex-rates-data", python_callable=download_rates
    )
