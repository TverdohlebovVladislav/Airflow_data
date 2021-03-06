import numpy as np
import pandas as pd
import datetime
import random


from .TableProductBase import TableProductBase

class Payment(TableProductBase):

    def __init__(self, counter: int = 1):
        super().__init__(counter)
        self.dataFrame = self.get_df()

    def get_df(self) -> pd.DataFrame:
        # For generating data
        payment_methods = ['bank card', 'from terminal', 'from another phone']

        # ---
        # Create dataframe - Payment
        count__to_start = self.id_counter
        payment_id_pk: list  = [i for i in range(count__to_start, self.max_count_costed_payment + count__to_start - 1)]
        customer_id_fk_in_payment: list = np.random.randint(1, self.max_count_customer, size=self.max_count_costed_payment - 1)

        # Payment method
        payment_method = np.random.choice(payment_methods, size=self.max_count_costed_payment-1, p=[0.7, 0.2, 0.1])

        # Date 
        date = []
        def random_date(start, end):
            """Generate a random datetime between `start` and `end`"""
            return start + datetime.timedelta(
                # Get a random amount of seconds between `start` and `end`
                seconds=random.randint(0, int((end - start).total_seconds())))

        d1 = datetime.datetime.strptime('01/01/2020 00:00 AM', '%m/%d/%Y %H:%M %p')
        d2 = datetime.datetime.strptime('12/31/2021 00:00 PM', '%m/%d/%Y %H:%M %p')
        for i in range(self.max_count_costed_payment - 1):
            rnd = random_date(d1, d2)
            date.append(rnd.strftime("%Y-%m-%d %I:%M %p"))
        date.sort(key=lambda lst: (lst[8:10], lst[2:5], lst[0:2], lst[-2::1], lst[-8:-6], lst[-5:-3]))

        # Amount
        middle = 0
        for i in range(1, self.max_count_product):
            if TableProductBase.get_df()['recurrent'][i] == 'regularly':
                middle += TableProductBase.get_df()['price'][i]
        middle /= self.max_count_product
        amount_once = np.random.choice([10, 15, 50, 90, 140], size=(self.max_count_costed_payment-1) // 3, p=[0.1, 0.3, 0.3, 0.2, 0.1])
        amount_reg = np.random.normal(loc=middle, scale=2.5, size=self.max_count_costed_payment-1 - (self.max_count_costed_payment-1) // 3)
        amount = np.hstack([amount_once, amount_reg])
        np.random.shuffle(amount)


        PaymentDf = pd.DataFrame(
            {
                "payment_id_pk": pd.Series(payment_id_pk, name="payment_id_pk", dtype="int"),
                "customer_id_fk": pd.Series(customer_id_fk_in_payment, name="customer_id_fk", dtype="int"),

                "payment_method": pd.Series(payment_method, name="payment_method", dtype="str"),
                "date_payment": pd.Series(date, name="date", dtype="str"),
                "amount": pd.Series(amount, name="customer_id_fk", dtype="float").round(),
            }
        )
        PaymentDf.set_index('payment_id_pk', inplace=True)
        return PaymentDf