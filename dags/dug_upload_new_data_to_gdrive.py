from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from os import getenv
from sqlalchemy import create_engine
from datetime import datetime
# import os
# import sys
# sys.path.insert(1, "~/airflow_canada_project/generate_data/")

# os.path.exists("~/airflow_canada_project/generate_data/")

# import generate_data.main
# from generate_data import main
# import generate_data.main as main

DAG_DEFAULT_ARGS = {
    'start_date': datetime(2020, 1, 1), 
    'depends_on_past': False
}
DEFAULT_POSTGRES_CONN_ID = "postgres_default"
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')

NOW_DATE = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
LAST_UPLOAD_DATE = ''

DAG_ID = "upload_new_data_to_drive"
schedule = "@hourly"

# ---
# Add time of upload 
def add_date_time_upl(df: pd.DataFrame) -> pd.DataFrame:
    df['date_of_upload'] = [NOW_DATE for i in range(len(df.index))]
    return df

# ---
# Load csv from example to DB
def load_csv_pandas(file_path: str, table_name: str, schema: str = "raw", conn_id: str = None) -> None:
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
    # extra = conn_object.extra_dejson
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}:{conn_object.port}/{conn_object.schema}"
         
    df = pd.read_csv(file_path)
    
    df_raw = pd.read_csv(os.path.join(RAW,customer_table_name+ext))
    LAST_UPLOAD_DATE = list(df_raw['date_of_upload'])[-1]

    customer_df = customer_df[customer_df.index > len(customer_df_raw.index) - 1]
    payments_df = payments_df[payments_df.index > len(payments_df_raw.index) - 1]

    customer_df = add_date_time_upl(customer_df)
    payments_df = add_date_time_upl(payments_df)

    customer_df = pd.concat([customer_df_raw, customer_df], ignore_index=True)
    payments_df = pd.concat([payments_df_raw, payments_df], ignore_index=True)

    engine = create_engine(jdbc_url)
    df.to_sql(table_name, engine, schema=schema, if_exists="replace")


# ---
# Datamart find
def datamart_pandas(table_name: str, schema: str = "datamart", conn_id: str = None) -> None:
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}:{conn_object.port}/{conn_object.schema}"
    engine = create_engine(jdbc_url)
    df = pd.read_sql("""
                    select c.customer_id, sum(p.amount) as amount, current_timestamp as execution_timestamp
                    from raw.customer as c
                    join raw.payments as p on c.customer_id=p.customer_id
                    group by c.customer_id
                    """,
                     engine)
    df.to_sql(table_name, engine, schema=schema, if_exists="append")


with DAG (
        dag_id=DAG_ID,
        description='Dag to simulate upload data to the google drive [version 1.0]',
        schedule_interval=schedule,
        default_args=DAG_DEFAULT_ARGS,
        is_paused_upon_creation=True,
        max_active_runs=1,
        catchup=False
    ) as dag:

    # Table names 
    customer_table_name = "customer"
    payments_table_name = "payments"
    datamart_table = "customer_totals"

    load_data_customer = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.RAW.{customer_table_name}",
        python_callable=load_csv_pandas,
        op_kwargs={
            "file_path": f"{AIRFLOW_HOME}/example/customer.csv",
            "table_name": customer_table_name,
            "conn_id": "raw_postgres"
        }
    )

    load_data_payment = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.RAW.{payments_table_name}",
        python_callable=load_csv_pandas,
        op_kwargs={
            "file_path": f"{AIRFLOW_HOME}/example/example.csv",
            "table_name": payments_table_name,
            "conn_id": "raw_postgres"
        }
    )

    data_mart = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.DATAMART.{datamart_table}",
        python_callable=datamart_pandas,
        op_kwargs={
            "table_name": datamart_table,
            "conn_id": "datamart_postgres"
        }
    )
    
    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)

    start_task >> [load_data_customer, load_data_payment] >>end_task