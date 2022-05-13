from typing import Optional

from airflow.hooks.base import BaseHook
from jinja2 import Template

from spark.utils.constants import DEFAULT_POSTGRES_CONN_ID


def get_query(filename: str, extra_vars: Optional[dict] = None) -> str:
    with open(filename, "r") as query_file:
        query_template = query_file.read()
    template = Template(query_template)
    return template.render(extra_vars)


def get_conn(conn_id=None):
    conn_object = BaseHook.get_connection(conn_id or DEFAULT_POSTGRES_CONN_ID)
    return f"postgresql://{conn_object.login}:{conn_object.password}@" \
           f"{conn_object.host}:{conn_object.port}"


custom_macros_dict = {
    "get_query": get_query,
    "get_conn": get_conn
}
