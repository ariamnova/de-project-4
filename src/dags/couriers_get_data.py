import time
import requests
import pendulum
import json
import pandas as pd
import psycopg2
import datetime

from datetime import timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from pymongo.mongo_client import MongoClient
from typing import List
from urllib.parse import quote_plus as quote
import logging
from bson.json_util import dumps


# get the airflow.task logger
logging.basicConfig(filename='info.log', level=logging.INFO)

#get parameters
sort_field = 'id'
sort_direction = 'asc'
limit = 50
offset = 0

#connection stuff
nickname = 'Nick'
cohort = '1'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-API-KEY': Variable.get('x_api_key')
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

# Create class to connect to MongoBD
class MongoConnect:
    def __init__(self,
                cert_path: str, 
                user: str,  
                pw: str,  
                hosts: List[str],  
                rs: str,  
                auth_db: str,  
                main_db: str  
                ) -> None:
        self.user = user
        self.pw = pw
        self.hosts = hosts
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path

    # Create string to connect to Mongo
    def url(self) -> str:
        return 'mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}'.format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=''.join(self.hosts),
            rs=self.replica_set,
            auth_src=self.auth_db)

    # Create client to BD
    def client(self):
            return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db] 

#get vars ro connect to MongoBD
cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
db_user = Variable.get("MONGO_DB_USER")
db_pw = Variable.get("MONGO_DB_PASSWORD")
hosts = Variable.get("MONGO_DB_HOST")
rs = Variable.get("MONGO_DB_REPLICA_SET")
db = Variable.get("MONGO_DB_DATABASE_NAME")

#get orders to check the offset
def orders_dag_read(ti):
    mongo_connect = MongoConnect(cert_path, db_user, db_pw, hosts, rs, db, db)
    dbs = mongo_connect.client()
    sort = [('fromm', 1)]
    myquery = { "date" : { "$gt" : fromm, "$lt" : to}}
    docs = list(dbs.get_collection("orders").find(myquery))
    num_of_orders = len(docs)
    ti.xcom_push(key='num_of_orders_per_month', value=num_of_orders)
    logging.info('We had {} orders for the previous month'.format(num_of_orders))
    return num_of_orders 

#get data via API and load it to staging

#get lists: couriers and restaurants
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
        logging.info('Get data for {}'.format(request))
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='de')
        for ins in final_list:
            ins=json.dumps(ins)
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
def get_delivery_data(ti):
    deliveries = []
    offset_range = ti.xcom_pull(key='num_of_orders_per_month') 
    for offset in range(0,offset_range+50,50):
        get_report_response = requests.get(
            f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?sort_field=date&from={fromm}&to={to}&sort_direction={sort_direction}&limit={limit}&offset={offset}", 
            headers=headers
            ).json()
        deliveries = deliveries + get_report_response
        if len(get_report_response) < 50:
            break
    logging.info('Get {} deliveries!'.format(len(deliveries)))
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn', schema='de')
    for ins in deliveries:
        ins = json.dumps(ins)
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
        schedule_interval ='*/60 * * * *',
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
    
    orders_dag_read = PythonOperator(
        task_id='orders_dag_read',
        python_callable=orders_dag_read,
    )


    (
        [orders_dag_read, get_rests_couriers]
        >> get_delivery_data
    )