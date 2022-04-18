import numpy as np
import pandas as pd

from data_generate_scripts.TableProductBase import TableProductBase

class Charge(TableProductBase):

    def __init__(self):
        self.dataFrame = self.get_df()

    def get_df(self) -> pd.DataFrame:
        charge_id_PK: list  = [i for i in range(1, self.max_count_costed_charge)] 
        product_instance_id_FK_charge: list   = np.random.randint(1, self.max_count_product_inst, size=self.max_count_costed_charge - 1)
        
        # ––––––––––––––––– charge_counter ––––––––––––––––––
        charge_counter = []
        for i in range(len(product_instance_id_FK_charge)):
            # We count and add to the charge_counter list the number of times the current element occurs
            charge_counter.append(np.count_nonzero(product_instance_id_FK_charge[:i] == product_instance_id_FK_charge[i]) + 1)

        # –––––––––––––––––––––– Date ––––––––––––––––––––––
        import datetime
        import random


        def random_date(start, end):
            """Generate a random datetime between `start` and `end`"""
            return start + datetime.timedelta(
                # Get a random amount of seconds between `start` and `end`
                seconds=random.randint(0, int((end - start).total_seconds())))

        date = []
        d1 = datetime.datetime.strptime('01/01/2020 00:00 AM', '%m/%d/%Y %H:%M %p')
        d2 = datetime.datetime.strptime('12/31/2021 00:00 PM', '%m/%d/%Y %H:%M %p')

        for i in range(len(product_instance_id_FK_charge)):
            rnd = random_date(d1, d2)
            date.append(rnd.strftime("%d/%m/%Y %I:%M %p").replace('/', '.'))

        date.sort(key=lambda lst: (lst[8:10], lst[2:5], lst[0:2], lst[-2::1], lst[-8:-6], lst[-5:-3]))
        
        # –––––––––––––––––––––– cost ––––––––––––––––––––––
        cost = []
        product_id_for_charge = []
        with open("data_source/ProductInstance.csv") as ProductInstance:
            ProductInstanceDf = pd.read_csv(ProductInstance, delimiter=',')
            for i in product_instance_id_FK_charge:
                product_id_for_charge.append(ProductInstanceDf['product_id_FK'][i - 1])
        with open("data_source/Product.csv") as Product:
            ProductDf = pd.read_csv(Product, delimiter=',')
            for i in product_id_for_charge:
                cost.append(ProductDf['Price'][i - 1])

        # –––––––––––––––––– event_type ––––––––––––––––––––
        event_type = [True if i < 13 else False for i in product_id_for_charge]

        # ################### Доработка #######################
        
        ChargeDf = pd.DataFrame(
            {
                "charge_id_PK": pd.Series(charge_id_PK, name="charge_id_PK", dtype="int"),
                "product_instance_id_FK": pd.Series(product_instance_id_FK_charge, name="product_instance_id_FK", dtype="int"),
                "charge_counter": pd.Series(charge_counter, name="charge_counter", dtype="int"),
                "date": pd.Series(date, name="date", dtype="str"),
                "cost": pd.Series(cost, name="cost", dtype="int"),
                "event_type": pd.Series(event_type, name="event_type", dtype="bool")
                # Add other columns here...
            }
        )
        
        ChargeDf.set_index('charge_id_PK', inplace=True)
        return ChargeDf
