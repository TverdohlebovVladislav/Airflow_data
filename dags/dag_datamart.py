from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from os import getenv
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
# from local_settings import postgresql as settings
from sqlalchemy.schema import CreateSchema

# 1. Загрузка данных в слой RAW
# 2. Сборка из слоя RAW в датамарт
# 3. Подключить ко всей этой красоте спарк 


DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}
DEFAULT_POSTGRES_CONN_ID = "clean_data"
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')
conn_object = BaseHook.get_connection(DEFAULT_POSTGRES_CONN_ID)
CONNECT_DB_DATA = [
    'airflow',
    'airflow',
    'postgres',
    '5432',
    'clean_data'
]

DAG_ID = "СREATE_DATAMART"
schedule = "@hourly"


def create_engine_session(user, passwd, host, port, db):
    '''
    Create engine with sqlalchemy to management DB
    '''
    url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
    if not database_exists(url):
        create_database(url)
    engine = create_engine(url, pool_size=50, echo=False)
    session = sessionmaker(bind=engine)()
    return session


def load_data_to_raw():
    '''
    Load data from clean_data DB to RAW
    '''
    conn = create_engine_session(*CONNECT_DB_DATA)
    statement = 'CREATE SCHEMA IF NOT EXISTS RAW'
    conn.execute(statement)
    conn.commit()
    conn.close()
    pass


def create_datamart():
    '''
    Create datamart from RAW
    '''
    pass


def create_simple_example_tables():
    '''
    Examples of requests to DB 
    '''
    pass


with DAG(dag_id=DAG_ID,
         description='Dag to transfer data from csv to postgres [version 1.0]',
         schedule_interval=schedule,
         default_args=DAG_DEFAULT_ARGS,
         is_paused_upon_creation=True,
         max_active_runs=1,
         catchup=False
         ) as dag:
    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)


    load_data_to_row = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.load_data_to_row",
        python_callable=load_data_to_raw
    )

    start_task >> load_data_to_row >> end_task

