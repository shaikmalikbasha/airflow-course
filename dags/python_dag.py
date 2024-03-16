from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def get_name(ti) -> str:
    ti.xcom_push(key="first_name", value="Malik Basha")
    ti.xcom_push(key="last_name", value="Shaik")


def get_age(ti):
    print(type(ti))
    ti.xcom_push(key="age", value="25")


def greet(ti) -> None:
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    age = ti.xcom_pull(task_ids="get_age", key="age")

    print("Hello World! Welcome to Apache Airflow Learning...")
    print(f"My name is {first_name} {last_name}, and I am of {age} year(s) old.")


default_args = {
    "owner": "shaikmalikbasha583",
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    default_args=default_args,
    dag_id="PythonDagId-v01",
    start_date=datetime(2023, 12, 31),
    schedule_interval="@once",
    description="Python Example Dag",
    catchup=False,
) as dag:
    """
    DAG to fetch and display Name and Age
    """

    task_1 = PythonOperator(
        task_id="greet",
        python_callable=greet,
        op_kwargs={"name": "Shaik Malik Basha", "age": 26},
    )

    task_2 = PythonOperator(task_id="get_name", python_callable=get_name)
    task_3 = PythonOperator(task_id="get_age", python_callable=get_age)

    [task_2, task_3] >> task_1
