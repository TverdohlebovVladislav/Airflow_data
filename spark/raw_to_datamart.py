import sys

from pyspark.sql import SparkSession, DataFrame
from utils.spark_utils import parse_spark_args


class RawToDatamart:

    def __init__(self, args: list):
        self.parsed_args = parse_spark_args(["table_name", "query", "jdbc_url"], args)
        self.table_name = self.parsed_args.table_name
        self.query = self.parsed_args.query
        self.jdbc_url = self.parsed_args.jdbc_url
        self.raw = "raw"
        self.datamart = "datamart"

    def run(self):
        spark = SparkSession.builder.appName("LocalFileToRaw").getOrCreate()
        conn_raw = f"{self.jdbc_url}/{self.raw}"
        conn_datamart = f"{self.jdbc_url}/{self.datamart}"
        spark.read.jdbc(conn_raw, "customer").alias("customer")
        spark.read.jdbc(conn_raw, "payment").alias("payment")

        df = spark.sql(self.query)

        df.write.jdbc(conn_datamart, table=self.table_name, mode="overwrite")


if __name__ == '__main__':
    RawToDatamart(sys.argv).run()
