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
import psycopg2
import core
import psycopg2.pool
import logging
import sqlalchemy.pool as pool

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

pool_config = {
    'host' : 'rc1b-w374pkyvwys3vlrp.mdb.yandexcloud.net',
    'port' : '6432',
    'dbname' : 'main',
    'user' : 'airflow',
    'password' : 'airflow$007',
    'target_session_attrs' : 'read-write',
    'sslmode' : 'verify-full',
}
conn = psycopg2.connect("""
    host=rc1b-w374pkyvwys3vlrp.mdb.yandexcloud.net
    port=6432
    dbname=main
    user=airflow
    password=airflow$007
    target_session_attrs=read-write
    sslmode=verify-full
""")

DAG_ID = "СREATE_DATAMART_FROM_RAW"
schedule = "@weekly"


def create_engine_session(user, passwd, host, port, db):
    '''
    Create engine with sqlalchemy to management DB
    '''
    mypool = pool.QueuePool(conn, max_overflow=10, pool_size=5)
    POOL = psycopg2.pool.ThreadedConnectionPool(0, 32, **pool_config)
    engine = create_engine('postgresql+psycopg2://', creator=POOL.getconn)

    # url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
    # if not database_exists(url):
    #     create_database(url)
    # engine = create_engine(url)
    session = sessionmaker(bind=engine)()
    return engine, session


def load_data_to_raw(table_name: str) -> None:
    '''
    Load data from clean_data DB to RAW
    table_name: str, add_mark_date_of_upload: boolean = False
    '''
    engine, conn = create_engine_session(*CONNECT_DB_DATA)
    statement = 'CREATE SCHEMA IF NOT EXISTS RAW; \n' + core.get_sql('raw')
    conn.execute(statement)
    conn.commit()
    conn.close()


    NOW_DATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # LAST_UPLOAD_DATE = ''
    print("Читаю...")
    df = pd.read_sql(
        f"""
        select *
        from public.{table_name}
        """,
        engine
    )
    logging.info("Прочитал...")

    df['date_of_upload'] = [NOW_DATE for i in range(len(df.index))]
    logging.info("Добавил даты...")

    df.drop_duplicates(list(df.columns.values)[:-1])
    logging.info("Дропнул дубликаты...")

    df.set_index(list(df.columns.values)[1], inplace=True)
    logging.info("Убрал ненужный столбец...")

    df.to_sql(table_name, engine, schema="raw", if_exists="append")
    logging.info("Теперь я в базе!")




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

    sql = f"""
        SELECT 
            {schema_from}.customer.customer_id,
            {schema_from}.customer.first_name,
            {schema_from}.customer.last_name,
            {schema_from}.customer.termination_date, 
            {schema_from}.customer.email, 
            {schema_from}.customer.age, 
            {schema_from}.customer.geo, 
            {schema_from}.customer.msisdn,
            {schema_from}.product.product_id,
            {schema_from}.product.allowance_data,
            {schema_from}.product.product_type,
            {schema_from}.costed_event.direction,
            {schema_from}.costed_event.event_type,
            {schema_from}.costed_event.calling_msisdn,
            {schema_from}.costed_event.date_costed_event as event_date, 
            {schema_from}.charge.date_charge as charge_date
        FROM {schema_from}.customer
        RIGHT JOIN {schema_from}.product_instance
            ON {schema_from}.customer.customer_id = {schema_from}.product_instance.customer_id_fk
        LEFT JOIN {schema_from}.product
            ON {schema_from}.product.product_id = {schema_from}.product_instance.product_id_fk
        RIGHT JOIN {schema_from}.costed_event
            ON {schema_from}.costed_event.product_instance_id_fk = {schema_from}.product_instance.product_instance_id_pk 
        RIGHT JOIN {schema_from}.charge
            ON {schema_from}.charge.product_instance_id_fk = {schema_from}.product_instance.product_instance_id_pk
        """
    if schema_from == 'raw':
        sql += """ WHERE {schema_from}.product.date_of_upload = (select date_of_upload from {schema_from}.product order by date_of_upload desc limit 1);"""

    df = pd.read_sql(
        sql,
        engine
    )
    df.to_sql(table_name, engine, schema='datamart', if_exists="replace")
    



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

    # dg_scr.castomer_const_find()
    #     dg_scr.ProductInstance().save_to_csv() 
    #     dg_scr.Customer().save_to_csv()
    #     dg_scr.CostedEvent().save_to_csv() 
    #     dg_scr.Charge().save_to_csv()
    #     dg_scr.Payment().save_to_csv()

    #  ----
    to_row_product = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.to_row_product",
        python_callable=load_data_to_raw,
        op_kwargs={
            "table_name": f"product",
        }
    )

    to_row_product_instance = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.to_row_product_instance",
        python_callable=load_data_to_raw,
        op_kwargs={
            "table_name": f"product_instance",
        }
    )

    to_row_customer = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.to_row_customer",
        python_callable=load_data_to_raw,
        op_kwargs={
            "table_name": f"customer",
        }
    )

    to_row_costed_event = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.to_row_costed_event",
        python_callable=load_data_to_raw,
        op_kwargs={
            "table_name": f"costed_event",
        }
    )

    to_row_charge = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.to_row_charge",
        python_callable=load_data_to_raw,
        op_kwargs={
            "table_name": f"charge",
        }
    )

    to_row_payment = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.to_row_payment",
        python_callable=load_data_to_raw,
        op_kwargs={
            "table_name": f"payment",
        }
    )


    # -----

    create_datamart_from = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.create_datamart_from",
        python_callable=create_datamart, 
        op_kwargs={
            "table_name": f"dashboard",
            "schema_from": 'public',
        }
    )

    start_task >> \
    [
        to_row_product,
        to_row_product_instance,
        to_row_customer,
        to_row_costed_event,
        to_row_charge,
        to_row_payment ,
    ]  >> \
    create_datamart_from >> \
    end_task

