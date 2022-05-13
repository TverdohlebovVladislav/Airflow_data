import sys

from pyspark.sql import SparkSession, DataFrame
from utils.spark_utils import parse_spark_args
from functools import reduce


class LocalFileToRaw:

    def __init__(self, args: list):
        self.parsed_args = parse_spark_args(["table_name", "paths", "jdbc_url"], args)
        self.table_name = self.parsed_args.table_name
        self.paths = (self.parsed_args.paths or "").split(",")
        self.jdbc_url = self.parsed_args.jdbc_url
        self.layer = "raw"

    def run(self):
        spark = SparkSession.builder.appName("LocalFileToRaw").getOrCreate()

        data_frames = [spark.read.csv(path, sep=",") for path in self.paths]
        df: DataFrame = reduce(DataFrame.union, data_frames).distinct().cache()

        df.write.jdbc(f"{self.jdbc_url}/{self.layer}", table=self.table_name, mode="overwrite")


if __name__ == '__main__':
    LocalFileToRaw(sys.argv).run()

