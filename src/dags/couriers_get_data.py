import time
import requests
import pendulum
import json
import pandas as pd
import psycopg2
import datetime

from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook


sort_field = 'id'
sort_direction = 'asc'
limit = 50
offset = 0

nickname = 'Nick'
cohort = '1'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
}

list_names = ['/restaurants', '/couriers']

#get date range for deliveries
today = datetime.date.today().replace(day=1)
prev_month = today - datetime.timedelta(days=1)
fromm = prev_month.replace(day=1)
to = prev_month
my_time = datetime.datetime.min.time()
fromm = datetime.datetime.combine(fromm, my_time)
to = datetime.datetime.combine(to, my_time)

#get data via API and load it to staging
#get sable lists: couriers and restaurants
def get_rests_couriers():
    for request in list_names:
        final_list = []
        for offset in range(0,100000,50):
            get_report_response = requests.get(
            f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net{request}?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}", 
            headers=headers
            ).json()
            final_list = final_list + get_report_response
            if len(get_report_response) < 50:
                break
        print('Get data for {}'.format(request))
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='de')
        for ins in final_list:
            pg_conn = pg_hook.get_conn()
            cursor = pg_conn.cursor()
            cursor.execute(
                            """INSERT INTO stg."{}"
                                (workflow_value)
                                VALUES (%(table_values)s)
                                ON CONFLICT (workflow_value) DO NOTHING;
                            """.format(str(request[1:])),
                            {
                                "table_values": str(ins),
                            }
                        )
            pg_conn.commit()
            pg_conn.close()

    return 200

#get delivery data
def get_delivery_data():
    deliveries = []
    for offset in range(0,100000,50):
        get_report_response = requests.get(
            f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?sort_field=date&from={fromm}&to={to}&sort_direction={sort_direction}&limit={limit}&offset={offset}", 
            headers=headers
            ).json()
        deliveries = deliveries + get_report_response
        if len(get_report_response) < 50:
            break
    print('Get {} deliveries!'.format(len(deliveries)))
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='de')
    for ins in deliveries:
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        cursor.execute(
                        """INSERT INTO stg.deliveries
                             (workflow_value, from_date, to_date)
                             VALUES (%(table_values)s, %(from_date_insert)s, %(to_date_insert)s)
                             ON CONFLICT (workflow_value) DO NOTHING;
                         """,
                        {
                            "table_values": str(ins),
                             "from_date_insert": fromm,
                             "to_date_insert": to
                         }
                    )
        pg_conn.commit()
        pg_conn.close()

    return 200
    
args = {
    "owner": "marina.ariamnova",
    'email': ['marina.ariamnova@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

business_dt = '{{ ds }}'


with DAG(
        'get_raw_data',
        default_args=args,
	schedule_interval = '0 8 1/10 * 1',
        catchup=True,
        start_date=pendulum.datetime(2022, 10, 22, tz="UTC"),
        max_active_runs = 1
) as dag:

    get_rests_couriers = PythonOperator(
        task_id='get_rests_couriers',
        python_callable=get_rests_couriers)
    
    get_delivery_data = PythonOperator(
        task_id='get_delivery_data',
        python_callable=get_delivery_data)


    (
        get_rests_couriers
        >> get_delivery_data
    )