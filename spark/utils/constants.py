ARGS_APP_NAME = "app_name"
ARGS_TABLE_NAME = "table_name"
ARGS_SOURCE_TABLE_NAMES = "src_table_names"
ARGS_PATHS = "paths"
ARGS_JDBC_URL = "jdbc_url"
ARGS_QUERY = "query"


DEFAULT_POSTGRES_CONN_ID = "postgres_default"

SPARK_CONF = {
    "spark.driver.cores": "1",
    "spark.driver.memory": "512m",
    "spark.executor.cores": "1",
    "spark.executor.instances": "2",
    "spark.executor.memory": "700m",
    "spark.dynamicAllocation.enabled": "false"
}
