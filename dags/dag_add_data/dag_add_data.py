# from debugpy import connect
from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from os import getenv
from sqlalchemy import create_engine
from datetime import datetime
import sys
import psycopg2
from psycopg2 import OperationalError
import os.path
from os.path import dirname, abspath
d = dirname(dirname(abspath(__file__)))
import data_generate_scripts as dg_scr
import sqlalchemy.pool as pool
import core

import oauth2client
import logging
import psycopg2.pool

conn = psycopg2.connect("""
    host=rc1b-w374pkyvwys3vlrp.mdb.yandexcloud.net
    port=6432
    dbname=main
    user=airflow
    password=airflow$007
    target_session_attrs=read-write
    sslmode=verify-full
""")
pool_config = {
    'host' : 'rc1b-w374pkyvwys3vlrp.mdb.yandexcloud.net',
    'port' : '6432',
    'dbname' : 'main',
    'user' : 'airflow',
    'password' : 'airflow$007',
    'target_session_attrs' : 'read-write',
    'sslmode' : 'verify-full',
}



DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}
DEFAULT_POSTGRES_CONN_ID = "clean_data"
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')

DAG_ID = "ADD_DATA_TO_CLEAN"
schedule = "@hourly"


def load_csv_pandas(file_path: str, table_name: str, schema: str = "raw", conn_id: str = None) -> None:
    """
    Load data from csv to DB
    """
    logging.info("НАЧИНАЮ ЗАГРУЗКУ ДАННЫХ")
    # conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
    # # extra = conn_object.extra_dejson
    # # jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
    # #            f"{conn_object.host}:{conn_object.port}/{conn_object.schema}"
    # jdbc_url = f"postgresql://airflow:airflow$007@" \
    #            f"rc1b-w374pkyvwys3vlrp.mdb.yandexcloud.net:6432/main"
    df = pd.read_csv(file_path)
    # engine = create_engine(jdbc_url)
    logging.info("ПРОЧИТАЛ ФАЙЛЫ")
    df.set_index(df.columns[0], inplace=True)
    logging.info("НАЧИНАЮ ЗАГРУЗКУ В БАЗУ")
    mypool = pool.QueuePool(conn, max_overflow=10, pool_size=5)
    POOL = psycopg2.pool.ThreadedConnectionPool(0, 32, **pool_config)

    engine = create_engine('postgresql+psycopg2://', creator=POOL.getconn)
    df.to_sql(table_name, engine, if_exists="append")
    logging.info("ЗАГРУЗИЛ В БАЗУ")


def create_connection(db_name, db_user, db_password, db_host, db_port):
    """
    Create connection to database
    """
    connection = None
    try:
        connection = conn
        logging.info("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        logging.info(f"The error '{e}' occurred")
    return connection


def create_database(connection, query):
    """
    Function wich create new data_base
    """
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except psycopg2.ProgrammingError as e:
        print(f"The error '{e}' occurred")


def execute_query(connection, query):
    """
    SQL request to db
    """
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        return cursor.execute(query)
    except OperationalError as e:
        print(f"The error '{e}' occurred")


def check_table_for_emptiness(table_name: str, conn_id: str = None) -> int:
    """
    Check table count rows
    """
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
    connection = create_connection('clean_data', conn_object.login, conn_object.password, conn_object.host, conn_object.port)
    query = "SELECT COUNT(*) FROM " + table_name

    cursor = connection.cursor()
    count_check =  cursor.execute(query)
    result = bool(cursor.fetchone()[0])
    return result
    


def create_db_for_clean_data(conn_id: str = None) -> None:
    """
    Create schema CLEAN, which consists all data
    """
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)

    # Connect to exists database
    connection = create_connection('bd1', conn_object.login, conn_object.password, conn_object.host, conn_object.port)
    create_database_query = "CREATE DATABASE clean_data"

    # Create clean db bi nit exists
    # create_database(connection, create_database_query)
    connetction_clean = create_connection('clean_data', conn_object.login, conn_object.password, conn_object.host, conn_object.port)

    # Create tables if they are not exists for first launch
    create_tables = core.get_sql('public')
    execute_query(connetction_clean, create_tables)


def create_csv_files():
    """
    Create CSV for the first upload if not exists
    """
    path_to_files = f"{AIRFLOW_HOME}/csv/"
    check = True
    file_names = [
        'Product.csv',
        'Customer.csv',
        'ProductInstance.csv'
        'CostedEvent.csv',
        'Charge.csv', 
        'Payment.csv',    
    ]
    for i in range(len(file_names)):
        if not os.path.exists(path_to_files + file_names[i]):
            check = False
            break
    if not check:
        dg_scr.castomer_const_find()
        dg_scr.ProductInstance().save_to_csv() 
        dg_scr.Customer().save_to_csv()
        dg_scr.CostedEvent().save_to_csv() 
        dg_scr.Charge().save_to_csv()
        dg_scr.Payment().save_to_csv()


def add_data_to_clean_db(conn_id: str = None) -> None:
    """
    Add demo data to DB if not exists
    """

    # Create csv if it not exists
    path_to_files = f"{AIRFLOW_HOME}/csv/"
    logging.info("НАЧИНАЮ ЧЕКАТЬ")
    if not check_table_for_emptiness('product'):
        load_csv_pandas(path_to_files + 'Product.csv', 'product', 'public')
    if not check_table_for_emptiness('customer'):
        load_csv_pandas(path_to_files + 'Customer.csv', 'customer', 'public')
    if not check_table_for_emptiness('product_instance'):
        load_csv_pandas(path_to_files + 'ProductInstance.csv', 'product_instance', 'public')
    if not check_table_for_emptiness('costed_event'):
        load_csv_pandas(path_to_files + 'CostedEvent.csv', 'costed_event', 'public')
    if not check_table_for_emptiness('charge'):
        load_csv_pandas(path_to_files + 'Charge.csv', 'charge', 'public')
    if not check_table_for_emptiness('payment'):
        load_csv_pandas(path_to_files + 'Payment.csv', 'payment', 'public')
        
    

def generate_new_data(conn_id: str = None) -> None:
    """
    Generate new data automaticly
    """
    # Set quantoty of data to autogenerate
    dg_scr.TableProductBase.max_count_costed_payment = 30
    dg_scr.TableProductBase.max_count_costed_charge = 10
    dg_scr.TableProductBase.max_count_costed_event = 50

    # Find max count to start FROM DB
    mypool = pool.QueuePool(conn, max_overflow=10, pool_size=5)
    POOL = psycopg2.pool.ThreadedConnectionPool(0, 32, **pool_config)

    engine = create_engine('postgresql+psycopg2://', creator=POOL.getconn)
    counted_rows = pd.read_sql(
        """
        SELECT COUNT(*) as count from public.payment
        UNION ALL SELECT COUNT(*) as count from public.charge
        UNION ALL SELECT COUNT(*) as count from public.costed_event
        """,
        engine
    )
    counted_rows_list = list(counted_rows['count'])

    # Generate data to upload
    pt = dg_scr.Payment(counted_rows_list[0] + 1)
    ch = dg_scr.Charge(counted_rows_list[1] + 1)
    ce = dg_scr.CostedEvent(counted_rows_list[2] + 1) 

    # Send data to the serv 
    pt.get_df().to_sql('payment', engine, schema='public', if_exists="append")
    ch.get_df().to_sql('charge', engine, schema='public', if_exists="append")
    ce.get_df().to_sql('costed_event', engine, schema='public', if_exists="append")


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

    customer_table_name = "customer"
    payments_table_name = "payments"
    datamart_table = "customer_totals"

    create_tables_func = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.create_tables_if_not_exists",
        python_callable=create_db_for_clean_data,
        op_kwargs={
            "conn_id": "raw_postgres"
        }
    )

    create_csv_if_not_exists = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.create_csv_if_not_exists",
        python_callable=create_csv_files,
    )

    add_data_to_tables_if_not_exist = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.add_data_to_tables_if_not_exist",
        python_callable=add_data_to_clean_db,
        op_kwargs={
            "conn_id": "clean_data"
        }
    )

    add_new_data = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.add_new_data_every_hour",
        python_callable=generate_new_data,
        op_kwargs={
            "conn_id": "clean_data"
        }
    )

    start_task >> create_tables_func >> create_csv_if_not_exists >> add_data_to_tables_if_not_exist >> add_new_data >> end_task
