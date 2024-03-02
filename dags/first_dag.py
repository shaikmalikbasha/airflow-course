from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "shaikmalikbasha583",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="my_first_dag_id_v3",
    default_args=default_args,
    description="First Dag with Description",
    start_date=datetime(2023, 12, 31, 2),
    schedule_interval="@daily",
) as dag:
    task_1 = BashOperator(
        task_id="first_task_id",
        bash_command="echo Hello, World! This is the first Command",
    )

    task_2 = BashOperator(
        task_id="second_task_id",
        bash_command="echo Hello, World! This is the second Command",
    )

    task_3 = BashOperator(
        task_id="third_task_id",
        bash_command="echo Hello, World! This is the third Command",
    )

    task_4 = BashOperator(
        task_id="fourth_task_id",
        bash_command="echo Hello, World! This is the fourth Command",
    )

    # ## Method - 1
    # task_1.set_downstream(task_2)
    # task_1.set_downstream(task_3)
    # task_4.set_upstream([task_2, task_3])

    # ## Method - 2
    # task_1 >> task_2
    # task_1 >> task_3

    # task_2 >> task_4
    # task_3 >> task_4

    ## Method - 3
    task_1 >> [task_2, task_3] >> task_4
