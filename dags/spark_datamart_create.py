from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
# from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
# from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from os import getenv
from datetime import datetime
from os.path import dirname, abspath
d = dirname(dirname(abspath(__file__)))
import core 

DAG_DEFAULT_ARGS = {'start_date': datetime(2020, 1, 1), 'depends_on_past': False}
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')

postgres_driver_jar = "/usr/local/spark/resources/postgresql-9.4.1207.jar"
DAG_ID = "SPARK"
schedule = "@hourly"

CONNECT_DB_DATA = [
    'airflow',
    'airflow',
    'postgres',
    '5432',
    'clean_data'
]



def get_sql_to_create_datamart(schema_from: str = 'raw'):
    '''
    Create datamart from RAW SQL
    '''
    sql = f"""
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
        """
    return sql
    


with DAG(dag_id=DAG_ID,
         description='Dag to transfer data from csv to postgres [version 1.0]',
         schedule_interval=schedule,
         default_args=DAG_DEFAULT_ARGS,
         is_paused_upon_creation=True,
         max_active_runs=1,
         catchup=False,
         render_template_as_native_obj=True,
         user_defined_macros=core.custom_macros_dict
         ) as dag:
    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)

    charge_table_name = "charge"
    product_instance_table_name = "product_instance"
    costed_event_table_name = "costed_event"
    customer_table_name = "customer"
    payments_table_name = "payment"
    product_table_name = "product"

    datamart_table = "datamart"

    # charge
    charge_raw_task_id = f"{DAG_ID}.RAW.{charge_table_name}"
    load_charge_raw_task = SparkSubmitOperator(
        dag=dag,
        task_id=charge_raw_task_id,
        application=f"/usr/local/spark/core/to_raw.py",
        application_args=core.build_spark_args(
            **{
                core.ARGS_APP_NAME: charge_raw_task_id,
                core.ARGS_TABLE_NAME: charge_table_name,
                core.ARGS_PATHS: f"/usr/local/spark/example/charge.csv",
                core.ARGS_JDBC_URL: "{{ get_conn() }}"
            }
        ),
        conf=core.SPARK_CONF,
        jars=postgres_driver_jar
    )

    # product_instance
    product_instance_raw_task_id = f"{DAG_ID}.RAW.{product_instance_table_name}"
    load_product_instance_raw_task = SparkSubmitOperator(
        dag=dag,
        task_id=product_instance_raw_task_id,
        application=f"/usr/local/spark/core/to_raw.py",
        application_args=core.build_spark_args(
            **{
                core.ARGS_APP_NAME: product_instance_raw_task_id,
                core.ARGS_TABLE_NAME: product_instance_table_name,
                core.ARGS_PATHS: f"/usr/local/spark/example/product_instance.csv",
                core.ARGS_JDBC_URL: "{{ get_conn() }}"
            }
        ),
        conf=core.SPARK_CONF,
        jars=postgres_driver_jar
    )

    # costed_event
    costed_event_raw_task_id = f"{DAG_ID}.RAW.{costed_event_table_name}"
    load_costed_event_raw_task = SparkSubmitOperator(
        dag=dag,
        task_id=costed_event_raw_task_id,
        application=f"/usr/local/spark/core/to_raw.py",
        application_args=core.build_spark_args(
            **{
                core.ARGS_APP_NAME: costed_event_raw_task_id,
                core.ARGS_TABLE_NAME: costed_event_table_name,
                core.ARGS_PATHS: f"/usr/local/spark/example/costed_event.csv",
                core.ARGS_JDBC_URL: "{{ get_conn() }}"
            }
        ),
        conf=core.SPARK_CONF,
        jars=postgres_driver_jar
    )

    # customer
    customer_raw_task_id = f"{DAG_ID}.RAW.{customer_table_name}"
    load_customer_raw_task = SparkSubmitOperator(
        dag=dag,
        task_id=customer_raw_task_id,
        application=f"/usr/local/spark/core/to_raw.py",
        application_args=core.build_spark_args(
            **{
                core.ARGS_APP_NAME: customer_raw_task_id,
                core.ARGS_TABLE_NAME: customer_table_name,
                core.ARGS_PATHS: f"/usr/local/spark/example/customer.csv",
                core.ARGS_JDBC_URL: "{{ get_conn() }}"
            }
        ),
        conf=core.SPARK_CONF,
        jars=postgres_driver_jar
    )

    # payment
    payments_raw_task_id = f"{DAG_ID}.RAW.{payments_table_name}-1"
    load_payments_raw_task = SparkSubmitOperator(
        dag=dag,
        task_id=payments_raw_task_id,
        application=f"/usr/local/spark/core/local_file_to_raw.py",
        application_args=core.build_spark_args(
            **{
                core.ARGS_APP_NAME: payments_raw_task_id,
                core.ARGS_TABLE_NAME: payments_table_name,
                core.ARGS_PATHS: f"/usr/local/spark/example/payments.csv",
                core.ARGS_JDBC_URL: "{{ get_conn() }}"
            }
        ),
        conf=core.SPARK_CONF,
        jars=postgres_driver_jar
    )

    # product
    product_raw_task_id = f"{DAG_ID}.RAW.{product_table_name}"
    load_product_raw_task = SparkSubmitOperator(
        dag=dag,
        task_id=product_raw_task_id,
        application=f"/usr/local/spark/core/local_file_to_raw.py",
        application_args=core.build_spark_args(
            **{
                core.ARGS_APP_NAME: product_raw_task_id,
                core.ARGS_TABLE_NAME: product_table_name,
                core.ARGS_PATHS: f"/usr/local/spark/example/payments.csv",
                core.ARGS_JDBC_URL: "{{ get_conn() }}"
            }
        ),
        conf=core.SPARK_CONF,
        jars=postgres_driver_jar
    )




    # CREATE_DATA_MART
    query_path = get_sql_to_create_datamart()
    customer_totals_datamart_task_id = f"{DAG_ID}.DATAMART.{datamart_table}"
    customer_totals_datamart_task = SparkSubmitOperator(
        dag=dag,
        task_id=customer_totals_datamart_task_id,
        application=f"/usr/local/spark/core/raw_to_datamart.py",
        application_args=core.build_spark_args(
            **{
                core.ARGS_APP_NAME: customer_totals_datamart_task_id,
                core.ARGS_TABLE_NAME: datamart_table,
                core.ARGS_JDBC_URL: "{{ get_conn() }}",
                core.ARGS_QUERY: "{{ get_query('"+query_path+"') }}"
            }
        ),
        conf=core.SPARK_CONF,
        jars=postgres_driver_jar
    )

    start_task >> \
    [
        load_charge_raw_task,
        load_product_instance_raw_task,
        load_costed_event_raw_task,
        load_customer_raw_task,
        load_payments_raw_task,
        load_product_raw_task
    ] >> \
    customer_totals_datamart_task >> \
    end_task