from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from os import getenv
from sqlalchemy import create_engine
from datetime import datetime

from generate_data import main

DAG_DEFAULT_ARGS = {
    'start_date': datetime(2020, 1, 1), 
    'depends_on_past': False
}
DEFAULT_POSTGRES_CONN_ID = "postgres_default"
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')

DAG_ID = "upload_new_data_to_drive"
schedule = "@hourly"

# ----
# Generate data_example
def generate_data_ex():
    main.generate_data()

# ---
# Update data in gogle drive 
def update_data_in_drive():
    pass

with DAG (
        dag_id=DAG_ID,
        description='Dag to simulate upload data to the google drive [version 1.0]',
        schedule_interval=schedule,
        default_args=DAG_DEFAULT_ARGS,
        is_paused_upon_creation=True,
        max_active_runs=1,
        catchup=False) as dag:

    load_data = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.LAOD_DATA",
        python_callable=generate_data_ex
        # op_kwargs={
        #     "file_path": f"{AIRFLOW_HOME}/example/customer.csv",
        #     "table_name": customer_table_name,
        #     "conn_id": "raw_postgres"
        # }
    )

    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)

    start_task >> end_task