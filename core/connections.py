from typing import Optional
from airflow.hooks.base import BaseHook
from jinja2 import Template
from argparse import ArgumentParser
from functools import reduce


ARGS_APP_NAME = "app_name"
ARGS_TABLE_NAME = "table_name"
ARGS_SOURCE_TABLE_NAMES = "src_table_names"
ARGS_PATHS = "paths"
ARGS_JDBC_URL = "jdbc_url"
ARGS_QUERY = "query"
DEFAULT_POSTGRES_CONN_ID = "clean_data"


SPARK_CONF = {
    "spark.driver.cores": "1",
    "spark.driver.memory": "512m",
    "spark.executor.cores": "1",
    "spark.executor.instances": "2",
    "spark.executor.memory": "700m",
    "spark.dynamicAllocation.enabled": "false"
}


def get_query(filename: str, extra_vars: Optional[dict] = None) -> str:
    with open(filename, "r") as query_file:
        query_template = query_file.read()
    template = Template(query_template)
    return template.render(extra_vars)


def get_conn(conn_id=None):
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
    return f"postgresql://{conn_object.login}:{conn_object.password}@" \
           f"{conn_object.host}:{conn_object.port}"


def parse_spark_args(keys: list, argv: list):
    """parse_spark_args.
    Parse agruments passed to spark submit operator
    :param keys: The keys for parameters passed
    :type keys: list
    :param argv: The sys.argv instance containing the list of keys and values
    :type argv: list
    """
    parser = ArgumentParser()
    for key in keys:
        parser.add_argument(('--' + key))
    parser.add_argument(('--' + "app_name"))
    return parser.parse_args(argv[1:])


def build_spark_args(**kwargs):
    return reduce(list.__add__, [[f"--{key}", value] for key, value in kwargs.items() if value])


custom_macros_dict = {
    "get_query": get_query,
    "get_conn": get_conn
}
