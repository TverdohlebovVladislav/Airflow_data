
import os
import re
import pickle
import requests
from tqdm import tqdm
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from os import getenv
import apiclient.discovery
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import httplib2
from googleapiclient.errors import HttpError

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


from pyspark.sql import SparkSession

DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')
CREDENTIALS_PATH = getenv('AIRFLOW_HOME', '/opt/airflow') + '/dags/add_data_from_drive/credentials.json'
DOWNLOAD_DATA = getenv('AIRFLOW_HOME', '/opt/airflow') + '/dags/add_data_from_drive/temp/'

DAG_ID = "ADD_DATA_FROM_DRIVE"
schedule = "@hourly"

# MODIFICATE LATER !
DEFAULT_POSTGRES_CONN_ID = "clean_data"
conn_object = BaseHook.get_connection(DEFAULT_POSTGRES_CONN_ID)
CONNECT_DB_DATA = [
    'airflow',
    'airflow',
    'postgres',
    '5432',
    'clean_data'
]
postgres_driver_jar = "/usr/local/spark/resources/postgresql-9.4.1207.jar"
# MODIFICATE LATER !


SCOPES = [
    'https://www.googleapis.com/auth/drive.metadata',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/spreadsheets.readonly'
]


def get_gdrive_service():
    """
    Get connetction to interactions
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, SCOPES)
    httpAuth = credentials.authorize(httplib2.Http())
    service = apiclient.discovery.build('drive', 'v3', http=httpAuth)
    return service


def search(service, query):
    """
    Search files in gdrive
    """
    # search for the file
    result = []
    page_token = None
    while True:
        response = service.files().list(q=query,
                                        spaces="drive",
                                        fields="nextPageToken, files(id, name, mimeType)",
                                        pageToken=page_token).execute()
        # iterate over filtered files
        for file in response.get("files", []):
            result.append((file["id"], file["name"], file["mimeType"]))
        page_token = response.get('nextPageToken', None)
        if not page_token:
            # no more files
            break
    return result


def download_file_from_google_drive(id, destination):
    """
    Download data from drive
    id - is an id of gdrive document - gives from search function 
    destination - is a path where will save file in local storage
    """
    def get_confirm_token(response):
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                return value
        return None

    def save_response_content(response, destination):
        CHUNK_SIZE = 32768
        # get the file size from Content-length response header
        file_size = int(response.headers.get("Content-Length", 0))
        # extract Content disposition from response headers
        content_disposition = response.headers.get("content-disposition")
        # parse filename
        filename = re.findall("filename=\"(.+)\"", content_disposition)[0]
        print("[+] File size:", file_size)
        print("[+] File name:", filename)
        progress = tqdm(response.iter_content(CHUNK_SIZE), f"Downloading {filename}", total=file_size, unit="Byte", unit_scale=True, unit_divisor=1024)
        with open(destination, "wb") as f:
            for chunk in progress:
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
                    # update the progress bar
                    progress.update(len(chunk))
        progress.close()

    # base URL for download
    URL = "https://docs.google.com/uc?export=download"
    # init a HTTP session
    session = requests.Session()
    # make a request
    response = session.get(URL, params = {'id': id}, stream=True)
    print("[+] Downloading", response.url)
    # get confirmation token
    token = get_confirm_token(response)
    if token:
        params = {'id': id, 'confirm':token}
        response = session.get(URL, params=params, stream=True)
    # download to disk
    save_response_content(response, destination)  


def download(filename):
    service = get_gdrive_service()
    # the name of the file you want to download from Google Drive 
    # search for the file by name
    search_result = search(service, query=f"name='{filename}'")
    # get the GDrive ID of the file
    file_id = search_result[0][0]
    # make it shareable
    service.permissions().create(body={"role": "reader", "type": "anyone"}, fileId=file_id).execute()
    # download file
    try:
        download_file_from_google_drive(file_id, DOWNLOAD_DATA + filename)
    except HttpError as err:
        print(err)
    

def update_db_data(schema: str = 'public', table_name: str = 'Product'):
    """
    Updating the database, based on files received from Google drive
    """
    # url = f"postgresql://{CONNECT_DB_DATA[0]}:{CONNECT_DB_DATA[1]}@{CONNECT_DB_DATA[2]}:{CONNECT_DB_DATA[3]}/{db}"
    # engine = create_engine(url, pool_size=50, echo=False)
    # session = sessionmaker(bind=engine)()

    spark = SparkSession \
        .builder \
        .appName("Load data from gdrive to DB") \
        .getOrCreate()
    df = spark.read.csv(DOWNLOAD_DATA)
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql:dbserver") \
        .option("dbtable", f"public.{table_name}") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .save()

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

    next_t = DummyOperator(task_id='NEXT')


    add_data_local_customer = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.upload_data_from_drive_CUSTOMER",
        python_callable=download,
        op_kwargs={
            "filename": "Customer.csv"
        }
    )

    add_data_local_product = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.upload_data_from_drive_PRODUCT",
        python_callable=download,
        op_kwargs={
            "filename": "Product.csv"
        }
    )

    # -------

    add_data_db_product = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.add_data_db_PRODUCT",
        python_callable=update_db_data,
        op_kwargs={
            "table_name": "Product"
        }
    )

    add_data_db_customer = PythonOperator(
        dag=dag,
        task_id=f"{DAG_ID}.add_data_db_CUSTOMER",
        python_callable=update_db_data,
        op_kwargs={
            "table_name": "Customer"
        }
    )

    # add_data_db_customer = SparkSubmitOperator(dag=dag,
    #         task_id=f"{DAG_ID}.add_data_db_CUSTOMER",
    #         application=f"/usr/local/spark/core/raw_to_datamart.py",
    #         application_args=build_spark_args(
    #             **{
    #                 ARGS_APP_NAME: customer_totals_datamart_task_id,
    #                 ARGS_TABLE_NAME: datamart_table,
    #                 ARGS_JDBC_URL: "{{ get_conn() }}",
    #                 ARGS_QUERY: "{{ get_query('"+query_path+"') }}"
    #             }
    #         ),
    #         conf=SPARK_CONF,
    #         jars=postgres_driver_jar)

    start_task >> \
    [add_data_local_customer, add_data_local_product] >> \
    add_data_db_product >> \
    add_data_db_customer >> \
    end_task


    # start_task >> \
    # [add_data_local_customer, add_data_local_product] >> \
    # next_t >> \ 
    # [add_data_db_product, add_data_db_customer] >> \
    # end_task
