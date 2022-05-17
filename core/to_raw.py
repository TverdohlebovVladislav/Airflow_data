from datetime import datetime
import sys

from pyspark.sql import SparkSession, DataFrame
from core.connections import parse_spark_args
from functools import reduce


class FileToRaw:

    def __init__(self, args: list):
        self.parsed_args = parse_spark_args(["table_name", "paths", "jdbc_url"], args)
        self.table_name = self.parsed_args.table_name
        self.paths = (self.parsed_args.paths or "").split(",")
        self.jdbc_url = self.parsed_args.jdbc_url
        self.clean = "public"
        self.raw = "raw"
        self.datamart = "datamart"

    def run(self):
        spark = SparkSession.builder.appName("LocalFileToRaw").getOrCreate()

        # Получить данные из имеющейся базы, добавить маркер загрузки
        conn_public = f"{self.jdbc_url}/{self.clean}"
        df = spark.read.jdbc(conn_public, self.table_name).alias(self.table_name)
        df_with_date_time = df.withColumn("date_time_upload", datetime.now)
        df_with_date_time.write.jdbc(f"{self.jdbc_url}/{self.raw}", table=self.table_name, mode="overwrite")


        # # Загрузить в слой raw
        # data_frames = [spark.read.csv(path, sep=",") for path in self.paths]
        # df: DataFrame = reduce(DataFrame.union, data_frames).distinct().cache()

        # df.write.jdbc(f"{self.jdbc_url}/{self.layer}", table=self.table_name, mode="overwrite")


if __name__ == '__main__':
    FileToRaw(sys.argv).run()