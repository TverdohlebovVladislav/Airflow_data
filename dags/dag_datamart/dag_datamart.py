from xmlrpc.client import boolean
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
schedule = "@weekly"


def create_engine_session(user, passwd, host, port, db):
    '''
    Create engine with sqlalchemy to management DB
    '''
    url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
    if not database_exists(url):
        create_database(url)
    engine = create_engine(url, pool_size=50, echo=False)
    session = sessionmaker(bind=engine)()
    return engine, session


def load_data_to_raw() -> None:
    '''
    Load data from clean_data DB to RAW

    table_name: str, add_mark_date_of_upload: boolean = False
    '''
    engine, conn = create_engine_session(*CONNECT_DB_DATA)
    statement = 'CREATE SCHEMA IF NOT EXISTS RAW'
    conn.execute(statement)

    # df_public = pd.read_sql(
    #     f"""
    #     select *
    #     from public.{table_name}
    #     """,
    #     engine
    # )

    # df_raw = pd.read_sql(
    #     f"""
    #     select *
    #     from raw.{table_name}
    #     """,
    #     engine
    # )

    # if add_mark_date_of_upload:
    #     pass

    conn.commit()
    conn.close()
    pass


def create_datamart(table_name: str, schema_from: str = 'raw', schema_to: str = 'datamart'):
    '''
    Create datamart from RAW
    '''
    engine, conn = create_engine_session(*CONNECT_DB_DATA)
    statement = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS datamart;

    """
    conn.execute(statement)
    conn.commit()
    conn.close()

    df = pd.read_sql(
        f"""
        SELECT 
            {schema_from}.customer.customer_id,
            {schema_from}.customer.first_name,
            {schema_from}.customer.last_name,
            {schema_from}.customer.termination_date, 
            {schema_from}.customer.email, 
            {schema_from}.customer.msisdn,
            {schema_from}.product.product_id,
            {schema_from}.product.allowance_data,
            {schema_from}.product.product_type,
            {schema_from}.costed_event.direction,
            {schema_from}.costed_event.event_type,
            {schema_from}.costed_event.calling_msisdn,
            {schema_from}.costed_event.date as event_date, 
            {schema_from}.charge.date as charge_date
        FROM customer
        RIGHT JOIN product_instance
            ON {schema_from}.customer.customer_id = {schema_from}.product_instance.customer_id_fk
        LEFT JOIN product
            ON {schema_from}.product.product_id = {schema_from}.product_instance.product_id_fk
        RIGHT JOIN costed_event
            ON {schema_from}.costed_event.product_instance_id_fk = {schema_from}.product_instance.product_instance_id_pk 
        RIGHT JOIN charge
            ON {schema_from}.charge.product_instance_id_fk = {schema_from}.product_instance.product_instance_id_pk
        """,
        engine
    )
    df.to_sql(table_name, engine, schema=schema_to, if_exists="replace")
    


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

    create_datamart_from = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.create_datamart_from",
        python_callable=create_datamart, 
        op_kwargs={
            "table_name": f"Datamart_test_version",
            "schema_from": 'public',
        }
    )

    start_task >> load_data_to_row >> create_datamart_from >> end_task

