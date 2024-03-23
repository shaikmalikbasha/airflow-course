import json
import logging

import httpx
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd

default_args = {"owner": "airflow"}


def get_objs(data, props, key):
    objs = []
    for obj in data[key]:
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
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
)
def httpx_example():
    logging.info("DAG has been successfully started...")

    @task()
    def get_api_data():
        res = httpx.get("https://jsonplaceholder.typicode.com/users")
        if res.status_code == 200:
            return res.json()

    @task()
    def process_users(data):
        users = []
        props = ["id", "name", "username", "email", "phone", "website"]
        for user in data:
            extracted_user = {}
            for prop in props:
                if prop in user.keys():
                    extracted_user[prop] = user[prop]
            users.append(extracted_user)
        return pd.DataFrame(users)

    @task()
    def process_addresses(data):
        addresses = []
        props = ["street", "suite", "city", "zipcode"]
        for user in data:
            extracted_address = {"user_id": user["id"]}
            for prop in props:
                if prop in user["address"].keys():
                    extracted_address[prop] = user["address"][prop]
                else:
                    extracted_address[prop] = None
                addresses.append(extracted_address)
        return pd.DataFrame(addresses)

    @task()
    def process_companies(data):
        companies = []
        props = ["name", "catchPhrase", "bs"]
        for user in data:
            extracted_address = {"user_id": user["id"]}
            for prop in props:
                if prop in user["company"].keys():
                    extracted_address[prop] = user["company"][prop]
                else:
                    extracted_address[prop] = None
                companies.append(extracted_address)
        return pd.DataFrame(companies)

    @task()
    def write_data_to_file(df: pd.DataFrame, file_name):
        df.to_csv(f"/opt/airflow/dags/files/{file_name}.csv", index=False, header=True)
        return True

    @task()
    def load_data_to_db(df, table_name):
        logging.info(f"DATA: {df}")
        pg_hook = PostgresHook(postgres_conn_id="postgres_docker_local")
        pg_hook.insert_rows(table=table_name, rows=df.values.tolist())
        return True

    @task()
    def send_report(**kwargs):
        logging.info("All tasks has been successfully completed!")

    data = get_api_data()

    users_df = process_users(data)
    addresses_df = process_addresses(data)
    companies_df = process_companies(data)

    write_data_to_file(users_df, "users")
    write_data_to_file(addresses_df, "addresses")
    write_data_to_file(companies_df, "companies")

    u = load_data_to_db(users_df, "users")
    a = load_data_to_db(addresses_df, "addresses")
    c = load_data_to_db(companies_df, "companies")
    send_report(u=u, a=a, c=c)


httpx_example()
