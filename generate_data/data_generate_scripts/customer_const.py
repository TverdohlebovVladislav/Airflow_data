import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from TableProductBase import TableProductBase

def castomer_const_find():
    def save_to_csv(dataFrame, name, path: str = "data_source/"):
        dataFrame.to_csv(path + name + ".csv") 

    # Count of data in every table
    max_count_customer: int = TableProductBase.max_count_customer

    # ---
    # 1. Create dataframe - Customer
    path_Customer = "file.xlsx"
    agree_for_promo_and_autopay_card = ['Yes', 'No']
    status_values = ['active', 'inactive']

    # ID
    customer_id_PK: list = [i for i in range(1, max_count_customer)]

    # Shows if customer has a stored card, from which operator can took money.
    autopay_card = np.random.choice(agree_for_promo_and_autopay_card, size=max_count_customer - 1, p=[0.5, 0.5])
    # Status
    status = np.random.choice(status_values, size=max_count_customer - 1, p=[0.9, 0.1])

    # Date of birth and customer_since
    # доработать, чтобы на момент первой покупки клиенту было 18
    date_start1 = datetime.strptime('01.01.1940', '%d.%m.%Y')
    data_of_birth = []
    for i in range(max_count_customer - 1):
        tmp1 = date_start1 + timedelta(random.randint(0, 23441))
        data_of_birth.append(tmp1.strftime("%d/%m/%Y").replace('/', '.'))

    CustomerDf = pd.DataFrame(
        {
            "customer_id": pd.Series(customer_id_PK, name="customer_id", dtype="int"),
            "autopay_card": pd.Series(autopay_card, name='autopay_card', dtype='str'),
            "status": pd.Series(status, name='status', dtype='str'),
            "data_of_birth": pd.Series(data_of_birth, name="data_of_birth", dtype="str"),
        }
    )

    CustomerDf.set_index('customer_id', inplace=True)
    save_to_csv(CustomerDf, 'customer_const')
