import pendulum
import psycopg2
import datetime
import logging

from datetime import timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook


log = logging.getLogger(__name__)
conn_id = 'postgres_conn' 

args = { 
    "owner": "marina.ariamnova", 
    'email': ['marina.ariamnova@gmail.com'], 
    'email_on_failure': False, 
    'email_on_retry': False, 
    'retries': 2 
} 

with DAG( 
        dag_id = 'couriers_data_db_upload',
        schedule_interval='*/30 * * * *',
        default_args=args, 
        catchup=False, 
        start_date=pendulum.datetime(2022, 11, 4, tz="UTC"),
        is_paused_upon_creation=False
) as dag: 
    
    externalsensor = ExternalTaskSensor(
        task_id='dag_completed_status',
        external_dag_id='get_raw_data',
        external_task_id=None,  # wait for whole DAG to complete
        check_existence=True,
        timeout=240)
    
    
    upload_to_dds = PostgresOperator( 
        task_id='upload_to_dds', 
        postgres_conn_id = conn_id,
        sql='sql/dds_insert.sql' 
    ) 
    
    upload_to_cdm = PostgresOperator( 
        task_id='upload_to_cdm', 
        postgres_conn_id = conn_id,
        sql='sql/cdm_insert.sql' 
    )
    
    (
        externalsensor
        >> upload_to_dds
        >> upload_to_cdm
    )