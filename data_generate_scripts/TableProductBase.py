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

    @staticmethod
    def get_df() -> pd.DataFrame:
        path_Product = f"{TableProductBase.AIRFLOW_HOME}/csv/Product.csv"
        ProductDf = pd.read_csv(path_Product, delimiter=',')
        ProductDf.set_index('product_id', inplace=True) 
        return ProductDf

    @staticmethod
    def get_max_count_product() -> int:
        return TableProductBase.get_df().shape[0]
    
    def save_to_csv(self, path: str = to_csv):
        if hasattr(self, "dataFrame"):
            self.dataFrame.to_csv(path + self.__class__.__name__ + ".csv") 
            print("Файл " + self.__class__.__name__ + ".csv" + " сохранен!")
        else:
            print("Ошибка сохранения файла!")

    def add_data_to_csv(self, path: str = to_csv):
        if hasattr(self, "dataFrame") and os.path.exists(self.__class__.__name__ + ".csv"):
            self.to_csv(path + self.__class__.__name__ + ".csv", mode='a')
            print("Файл " + self.__class__.__name__ + ".csv" + " обновлен!")
        else:
            self.save_to_csv()

TableProductBase.max_count_product = TableProductBase.get_max_count_product()