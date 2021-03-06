import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from .TableProductBase import TableProductBase

class Customer(TableProductBase):

    def __init__(self):
        self.dataFrame = self.get_df()

    def get_df(self) -> pd.DataFrame:
        customer = pd.read_csv(TableProductBase.AIRFLOW_HOME + "/csv/customer_const.csv")
        customer_id_pk = list(customer["customer_id"])
        autopay_card = list(customer["autopay_card"])
        status = list(customer["status"])
        data_of_birth = list(customer["data_of_birth"])
        termin_date = pd.read_csv(TableProductBase.AIRFLOW_HOME + "/csv/ProductInstance.csv")
        termination_date = list (termin_date['termination_date'])
        
        # 1. Create dataframe - Customer
        path_Customer = "file.xlsx"
        f_names_m = ['Liam', 'Jackson', 'Noah', 'Lucas', 'Oliver', 'Grayson', 'Leo', 'Jack', 'Benjamin', 'William', 'Luca',
                    'Logan', 'Ethan', 'Levi', 'James', 'Henry', 'Mateo', 'Jacob', 'Elliot', 'Mason', 'Miles', 'Theodore',
                    'Nathan', 'Owen', 'Alexander']
        f_names_w = ['Olivia', 'Emma', 'Mia', 'Sophia', 'Zoey', 'Charlotte', 'Amelia', 'Aria', 'Mila', 'Hannah', 'Ava', 'Chloe',
                    'Ella', 'Abigail', 'Everly', 'Leah', 'Nora', 'Ellie', 'Isabella', 'Riley', 'Avery', 'Lily', 'Isla',
                    'Scarlett',
                    'Charlie']
        l_names = ['Anderson', 'Beaulieu', 'Belanger', 'Bouchard', 'Cameron', 'Campbell', 'Chen', 'Cote', 'Davis', 'Evans',
                'Fortin', 'Gagnon',
                'Gauthier', 'Gill', 'Girard', 'Graham', 'Grant', 'Hall', 'Harris', 'Jackson', 'Johnson', 'Johnston',
                'Kennedy', 'Khan', 'King',
                'Landry', 'Lapointe', 'Lavoie', 'Leblanc', 'Levesque', 'Lewis', 'Lin', 'MacDonald', 'Martin', 'Mitchell',
                'Michaud', 'Morin',
                'Morrison', 'Murphy', 'Murray', 'Nadeau', 'Nguyen', 'Ouellet', 'Patel', 'Pelletier', 'Peters', 'Reynolds',
                'Richard', 'Rogen', 'Robinson']
        gender_values = ['female', 'male']
        language_values = ['English', 'French', 'Chinese']
        agree_for_promo_and_autopay_card = [1, 0]
        customer_category_values = ['business', ' physical']
        email_values = ['@aol.com', '@gmail.com', '@yahoo.com', '@gmx.com', '@hotmail.com' ,'@mail.com']
        
        # Gender
        gender = np.random.choice(gender_values, size=TableProductBase.max_count_customer - 1, p=[0.5, 0.5])

        # First name and Last name, email
        first_name, last_name, email = [], [], []
        for i in range(len(gender)):
            # If female
            if gender[i] == gender_values[0]:
                first_name.append(np.random.choice(f_names_w))
            # If male
            if gender[i] == gender_values[1]:
                first_name.append(np.random.choice(f_names_m))
            last_name.append(np.random.choice(l_names))
            check = True 
            while check:
                email.append(first_name[i] + '.' + last_name[i] + str(random.randint(0, 99999)) + np.random.choice(email_values))
                if len(set(email)) == len(email):
                    check = False
        
        # Language
        language = np.random.choice(language_values, size=TableProductBase.max_count_customer - 1, p=[0.76, 0.21, 0.03])
        # Refusal (Yes) or agreement (No) to participate in marketing promotions
        agree_for_promo = np.random.choice(agree_for_promo_and_autopay_card, size=TableProductBase.max_count_customer - 1, p=[0.5, 0.5])
        # Customer type: Business or physical
        customer_category = np.random.choice(customer_category_values, size=TableProductBase.max_count_customer - 1, p=[0.1, 0.9])

        # Customer_since
        date_start = datetime.strptime('01/01/2020', '%d/%m/%Y')
        customer_since = []
        for i in range(TableProductBase.max_count_customer - 1):
            tmp = date_start + timedelta(random.randint(0, 731))
            # customer_since.append(tmp.strftime("%d/%m/%Y"))
            customer_since.append(tmp.strftime("%Y-%m-%d"))

        
        # region
        code_and_region = {'Ontario': [226, 249, 289, 343, 365, 416, 437, 519, 548, 613, 647, 705, 807, 905],
                   'Quebec': [	367, 418, 438, 450, 514, 579, 581, 819, 873],
                   'British Columbia': [236, 250, 604, 672, 778],
                   'Alberta': [403, 587, 780, 825], 'Manitoba': [204, 431], 'Saskatchewan': [306, 639],
                   'Nova Scotia': [782, 902], 'New Brunswick': 506, 'Newfoundland and Labrador': 709, 
                   'Prince Edward Island': [782, 902], 
                   'Northwest Territories': 867, 'Yukon': 867, 'Nunavut': 867}

        check = True 
        while check: # ????????????????: ?????? ???? ???????????????????? ??????????????
            region = np.random.choice(list(code_and_region.keys()), size=TableProductBase.max_count_customer - 1, 
                                        p=[0.38, 0.23, 0.14, 0.11, 0.04, 0.03, 0.03, 0.02, 0.013, 0.004, 0.001, 0.001, 0.001])
            phone_number = []
            for i in region:
                if type(code_and_region[i]) == list:
                    phone_number.append('+1' + str(random.choice(code_and_region[i])) + str(random.randint(1000000,9999999)))
                else:
                    phone_number.append('+1' + str(code_and_region[i]) + str(random.randint(1000000,9999999)))
                
            # ???????? ???????????????????? ?????????????? ??????, ?????????????????? ????????, ???????? ???????? - ??????????????????
            if len(set(phone_number)) == len(phone_number):
                check = False

        # ????????????????????
        reg = {'Ontario':'[43.684345,-79.431292]', 'Quebec':'[46.807102,-71.211788]', 
       'British Columbia':'[48.423095,-123.366788]', 'Alberta':'[53.530798,-113.511802]', 
       'Manitoba':'[49.887898, -97.134185]', 'Saskatchewan':'[50.464050, -104.605617]', 
       'Nova Scotia':'[44.653761, -63.601324]', 'New Brunswick':'[45.956694, -66.658084]', 
       'Newfoundland and Labrador':'[47.561796, -52.712189]',
       'Prince Edward Island':'[46.252084, -63.138242]', 'Northwest Territories':'[62.455067, -114.426002]', 
       'Yukon':'[60.735414, -135.083119]', 'Nunavut':'[63.751499, -68.523283]'}
        geo = []
        for rg in region:
            if rg in region:
                geo.append(reg[rg])
            else:
                geo.append(None)
        
        # ??????-???? ???????????? ??????
        age = []
        n_date = []
        for date in data_of_birth:
            n_date.append(date[:4])

        CustomerDf = pd.DataFrame(
            {
                "customer_id": pd.Series(customer_id_pk, name="customer_id", dtype="int"),
                "customer_id": pd.Series(customer_id_pk, name="customer_id", dtype="int"),
                "first_name": pd.Series(first_name, name="first_name", dtype="str"),
                "last_name": pd.Series(last_name, name="last_name", dtype="str"),
                "gender": pd.Series(gender, name="gender", dtype="str"),

                "age": pd.Series(n_date, name="age", dtype="str"),

                "language": pd.Series(language, name='language', dtype='str'),
                "agree_for_promo": pd.Series(agree_for_promo, name='agree_for_promo', dtype='str'),
                "autopay_card": pd.Series(autopay_card, name='autopay_card', dtype='str'),
                "customer_category": pd.Series(customer_category, name='customer_category', dtype='str'),
                "status": pd.Series(status, name='status', dtype='str'),
                "data_of_birth": pd.Series(data_of_birth, name="data_of_birth", dtype="str"),
                "customer_since": pd.Series(customer_since, name="customer_since", dtype="str"),
                "email" : pd.Series(email, name="email", dtype="str"),

                "region": pd.Series(region, name="email", dtype="str"),
                "geo": pd.Series(geo, name="geo", dtype="str"),

                "termination_date": pd.Series(termination_date,name = "termination_date", dtype = "str"),
                "msisdn": pd.Series(phone_number,name = "msisdn", dtype = "str"),
                
            }
        )
        CustomerDf.set_index('customer_id', inplace=True)
        return CustomerDf
    