import json
import logging

import httpx
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {"owner": "airflow"}


def get_objs(data, props):
    objs = []
    for obj in data:
        extracted_user = {}
        for prop in props:
            if prop in obj.keys():
                extracted_user[prop] = obj[prop]
            else:
                extracted_user[prop] = None
        objs.append(extracted_user)
    return objs


@dag(
    dag_id="httpx-etl-example",
    description="Sample DAG that fetches data from API and write it to Postgres",
    schedule_interval="@daily",
    start_date=days_ago(2),
    default_args=default_args,
)
def httpx_example():
    logging.info("DAG has been successfully started...")

    @task()
    def extract_data():
        res = httpx.get("https://jsonplaceholder.typicode.com/users")
        if res.status_code == 200:
            return res.json()

    @task()
    def process_users(data):
        print(f"Type of Data: {type(data)}")
        logging.warning(f"Type of Data: {type(data)}")
        users = []
        props = ["id", "name", "username", "email", "phone", "website"]
        for user in data:
            extracted_user = {}
            for prop in props:
                if prop in user.keys():
                    extracted_user[prop] = user[prop]
            users.append(extracted_user)
        return users

    @task()
    def process_addresses(data):
        props = ["user_id", "street", "suite", "city", "zipcode"]
        return get_objs(data, props)

    @task()
    def process_companies(data):
        props = ["name", "catchPhrase", "bs"]
        return get_objs(data, props)

    data = extract_data()
    users = process_users(data)
    addresses = process_addresses(data)
    companies = process_companies(data)


httpx_example()
