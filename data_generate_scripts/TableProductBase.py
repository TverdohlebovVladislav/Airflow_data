import pandas as pd
import os
from os import getenv

class TableProductBase():
    
    max_count_customer: int = 1400
    max_count_product_inst: int = 1000
    max_count_costed_event: int = 1000
    max_count_costed_charge: int= 1000
    max_count_costed_payment: int = 24000 
    AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')
    to_csv = AIRFLOW_HOME + "/csv/"
    to_csv_temp = AIRFLOW_HOME + "/csv/temp/"

    def __init__(self, counter: int = 1):
        self.id_counter = counter

    @staticmethod
    def get_df() -> pd.DataFrame:
        path_Product = f"{TableProductBase.AIRFLOW_HOME}/csv/Product.csv"
        ProductDf = pd.read_csv(path_Product, delimiter=',')
        ProductDf.set_index('product_id', inplace=True) 
        return ProductDf

    @staticmethod
    def get_max_count_product() -> int:
        return TableProductBase.get_df().shape[0]
    
    def save_to_csv(self, path: str = to_csv) -> None:
        if hasattr(self, "dataFrame"):
            print(path)
            self.dataFrame.to_csv(path + self.__class__.__name__ + ".csv") 
            print("Файл " + self.__class__.__name__ + ".csv" + " сохранен!")
        else:
            print("Ошибка сохранения файла!")

    def add_data_to_csv(self, path: str = to_csv_temp) -> None:

        if not os.path.exists(path):
                os.mkdir(path)

        if hasattr(self, "dataFrame"):
            self.dataFrame.to_csv(path + self.__class__.__name__ + ".csv") 
            print("Файл " + self.__class__.__name__ + ".csv" + " сохранен!")
        else:
            print("Ошибка сохранения файла!")

TableProductBase.max_count_product = TableProductBase.get_max_count_product()