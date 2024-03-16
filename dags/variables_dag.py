import json
import logging
import pprint
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "Airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def set_default_config(ti):
    cuebiq_source_config = Variable.get("cuebiq-config", deserialize_json=True)
    logging.info(cuebiq_source_config)
    ti.xcom_push(key="cuebiq_source_config", value=cuebiq_source_config)


def log_config_details(ti):
    cuebiq_source_config = ti.xcom_pull(
        task_ids="set_default_config", key="cuebiq_source_config"
    )
    logging.info(f"XCOM_Values: {cuebiq_source_config}")

    logging.info(
        f"""
            {pprint.pformat(Variable.get("cuebiq-config", deserialize_json=True))}
        """
    )


with DAG(
    dag_id="variables-example",
    description="Same DAG with Variables",
    start_date=days_ago(2),
    schedule="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    logging.info("Running variables-example DAG")
    get_source_config = PythonOperator(
        task_id="get-source-config",
        python_callable=set_default_config,
    )

    log_config = PythonOperator(
        task_id="log-source-config", python_callable=log_config_details
    )

    get_source_config >> log_config
