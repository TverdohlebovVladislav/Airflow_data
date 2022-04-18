# from debugpy import connect
from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from os import getenv
from sqlalchemy import create_engine
from datetime import datetime
import os.path

import psycopg2
from psycopg2 import OperationalError

# При импорте - конфликты
# from data.funcs import generate_data


DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}
DEFAULT_POSTGRES_CONN_ID = "postgres_default"
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')

DAG_ID = "ADD_DATA_TO_CLEAN"
schedule = "@hourly"


def create_connection(db_name, db_user, db_password, db_host, db_port):
    """
    Create connection to database
    """
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
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
    return execute_query(connection, query)


def create_db_for_clean_data(conn_id: str = None) -> None:
    """
    Create schema CLEAN, which consists all data
    """
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)

    # Connect to exists database
    connection = create_connection('airflow', conn_object.login, conn_object.password, conn_object.host, conn_object.port)
    create_database_query = "CREATE DATABASE clean_data"

    # Create clean db bi nit exists
    create_database(connection, create_database_query)
    connetction_clean = create_connection('clean_data', conn_object.login, conn_object.password, conn_object.host, conn_object.port)

    # Create tables if they are not exists for first launch
    create_tables = """

        CREATE TABLE IF NOT EXISTS product (
            product_id SERIAL PRIMARY KEY,
            product_name TEXT NOT NULL, 
            product_category TEXT,
            product_type TEXT,
            recurrent TEXT,
            cost_for_call DECIMAL,
            cost_for_sms DECIMAL,
            cost_for_data DECIMAL,
            allowance_sms TEXT,
            allowance_voice TEXT,
            allowance_data TEXT,
            Price DECIMAL
        );
        CREATE TABLE IF NOT EXISTS customer (
            customer_id SERIAL PRIMARY KEY,
            first_name TEXT NOT NULL, 
            last_name TEXT NOT NULL,
            gender TEXT NOT NULL,
            language TEXT, 
            agree_for_promo BOOLEAN,
            autopay_card TEXT,
            customer_category TEXT, 
            status TEXT,
            data_of_birth DATE,
            customer_since TEXT, 
            email TEXT,
            region TEXT,
            termination_date DATE, 
            MSISDN TEXT
        );
        CREATE TABLE IF NOT EXISTS product_instance (
            product_instance_id_PK SERIAL PRIMARY KEY,
            customer_id_FK INTEGER REFERENCES customer(customer_id) NOT NULL, 
            product_id_FK INTEGER REFERENCES product(product_id) NOT NULL,
            activation_date DATE,
            termination_date DATE, 
            status INTEGER,
            distribution_channel TEXT
        );
        CREATE TABLE IF NOT EXISTS costed_event (
            event_id_PK SERIAL PRIMARY KEY,
            product_instance_id_FK INTEGER REFERENCES product_instance(product_instance_id_PK) NOT NULL, 
            calling_msisdn INTEGER,
            called_msisdn INTEGER,
            date TIMESTAMP, 
            cost DECIMAL,
            duration INTEGER,
            number_of_sms INTEGER, 
            number_of_data INTEGER,
            event_type TEXT,
            direction TEXT, 
            roaming BOOLEAN
        );
        CREATE TABLE IF NOT EXISTS charge (
            charge_id_PK SERIAL PRIMARY KEY,
            product_instance_id_FK INTEGER REFERENCES product_instance(product_instance_id_PK) NOT NULL, 
            charge_counter INTEGER,
            date TIMESTAMP,
            cost DECIMAL, 
            event_type BOOLEAN
        );
        CREATE TABLE IF NOT EXISTS payment (
            payment_id_PK SERIAL PRIMARY KEY,
            customer_id_FK INTEGER REFERENCES customer(customer_id) NOT NULL, 
            payment_method TEXT,
            date TIMESTAMP,
            amount DECIMAL
        );
    """
    execute_query(connetction_clean, create_tables)


def add_data_to_clean_db() -> None:
    """
    Add demo data to DB if not exists
    """
    # Create csv if it not exists
    data_soure = 'data/data_source/'
    check = True
    file_names = [
        'Charge.csv', 
        'CostedEvent.csv',
        'Customer.csv',
        'Payment.csv',
        'Product.csv',
        'ProductInstance.csv'
    ]
    for i in range(len(file_names)):
        if not os.path.exists(data_soure + file_names[i]):
            check = False
            break
        
    if check:  

        # jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
        #        f"{conn_object.host}:{conn_object.port}/{conn_object.schema}"
        # engine = create_engine(jdbc_url)
        # df = pd.read_sql("""
        #                 select c.customer_id, sum(p.amount) as amount, current_timestamp as execution_timestamp
        #                 from raw.customer as c
        #                 join raw.payments as p on c.customer_id=p.customer_id
        #                 group by c.customer_id
        #                 """,
        #                 engine)
        # df.to_sql(table_name, engine, schema=schema, if_exists="append")
        pass
    else: 

        # ДОДЕЛАТЬ АВТОМАТИЧЕСКУЮ ГЕНЕРАЦИЮ
        # generate_data()
        pass
    
    



    # Add data from csv 
    
    



# def create_tables():
#     """
#     Create tables for DB if it is not exist
#     """
#     pass


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
        task_id=f"{DAG_ID}.RAW.{customer_table_name}",
        python_callable=create_db_for_clean_data,
        op_kwargs={
            "conn_id": "raw_postgres"
        }
    )

    start_task >> create_tables_func >> end_task
