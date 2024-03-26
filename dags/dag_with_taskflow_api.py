from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    "owner": "Airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    default_args=default_args,
    dag_id="PythonDagWithTaskFlowAPI-1",
    description="Python DAG with TaskFlow API",
    start_date=datetime(2023, 12, 31),
    schedule_interval="@daily",
)
def first_etl():
    @task(multiple_outputs=True)
    def get_name():
        return {"first_name": "Malik", "last_name": "Shaik"}

    @task()
    def get_age():
        return 26

    @task()
    def greet(first_name: str, last_name: str, age: int):
        print(f"My name is {first_name} {last_name}, and I am of {age} year(s) old.")

    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict["first_name"], last_name=name_dict["last_name"], age=age)


first_etl()
