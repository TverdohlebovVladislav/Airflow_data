import pandas as pd

class TableProductBase():
    
    max_count_customer: int = 1400
    max_count_product_inst: int = 1000
    max_count_costed_event: int = 1000
    max_count_costed_charge: int= 1000
    max_count_costed_payment: int = 24000 

    @staticmethod
    def get_df() -> pd.DataFrame:
        path_Product = "data_source/Product.csv"
        ProductDf = pd.read_csv(path_Product, delimiter=',')
        ProductDf.set_index('product_id', inplace=True) 
        return ProductDf

    @staticmethod
    def get_max_count_product() -> int:
        return TableProductBase.get_df().shape[0]
    
    def save_to_csv(self, path: str = "data_source/"):
        if hasattr(self, "dataFrame"):
            self.dataFrame.to_csv(path + self.__class__.__name__ + ".csv") 
            print("Файл " + self.__class__.__name__ + ".csv" + " сохранен!")
        else:
            print("Ошибка сохранения файла!")

TableProductBase.max_count_product = TableProductBase.get_max_count_product()