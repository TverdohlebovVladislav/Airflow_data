import numpy as np
import pandas as pd
import datetime
import random


from .TableProductBase import TableProductBase

class CostedEvent(TableProductBase):

    def __init__(self, counter: int = 1):
        super().__init__(counter)
        self.dataFrame = self.get_df()

    def get_df(self) -> pd.DataFrame:
        # For generate data
        event_types = ['call', 'sms', 'data']
        direction_variations = ['incoming', 'outgoing']

        productDF = TableProductBase.get_df()

        products_with_call_cost = []
        products_with_sms_cost = []
        products_with_data_cost = []
        for i in range(TableProductBase.get_max_count_product()):
            if not pd.isnull(productDF)['cost_for_call'][i + 1]:
                # print(productDF['cost_for_call'][i + 1])
                products_with_call_cost.append(productDF.index[i])
            if not pd.isnull(productDF)['cost_for_sms'][i + 1]:
                products_with_sms_cost.append(productDF.index[i])
            if not pd.isnull(productDF)['cost_for_data'][i + 1]:
                products_with_data_cost.append(productDF.index[i])
        
        with open(TableProductBase.AIRFLOW_HOME + "/csv/ProductInstance.csv") as ProductInstance:
            pr_insDF = pd.read_csv(ProductInstance, delimiter=',')

        count__to_start = self.id_counter

        # ---
        # Generation columns for dataset 

        # ID
        event_id_pk: list  = [i for i in range(count__to_start, self.max_count_costed_event + count__to_start - 1)] 

        # From
        calling_msisdn: list = np.random.randint(1, self.max_count_customer, size=self.max_count_costed_event - 1)

        # To
        called_msisdn = []
        for i in range(len(calling_msisdn)):
            called_msisdn_el = np.random.randint(1, self.max_count_customer)
            while i == called_msisdn:
                called_msisdn_el = np.random.randint(1, self.max_count_customer)
            called_msisdn.append(called_msisdn_el)

        # Event type
        event_type = np.random.choice(event_types, size=self.max_count_costed_event-1, p=[0.6, 0.15, 0.25])

        # fk
        product_instance_id_fk_from_ce: list  = []
        #product_instanceDF = pd.read_csv('data_source/ProductInstance.csv', delimiter=',')
        for i in range(len(event_type)):
            
            # if is CALL
            if event_type[i] == event_types[0]:
                product_el = np.random.choice(products_with_call_cost)
                el_to_add = pr_insDF[pr_insDF.product_id_fk == product_el].product_instance_id_pk
                product_instance_id_fk_from_ce.append(np.random.choice(list(el_to_add)))
            # if is SMS
            if event_type[i] == event_types[1]:
                product_el = np.random.choice(products_with_sms_cost)
                el_to_add = pr_insDF[pr_insDF.product_id_fk == product_el].product_instance_id_pk
                product_instance_id_fk_from_ce.append(np.random.choice(list(el_to_add)))
            # if is DATA
            if event_type[i] == event_types[2]:
                product_el = np.random.choice(products_with_data_cost)
                el_to_add = pr_insDF[pr_insDF.product_id_fk == product_el].product_instance_id_pk
                product_instance_id_fk_from_ce.append(np.random.choice(list(el_to_add)))


        # Direction
        direction = []
        for i in range(len(event_type)):
            # if is not DATA
            if event_type[i] != event_types[2]:
                direction.append(np.random.choice(direction_variations))
            else: 
                direction.append(np.NaN)

        # Roaming
        roaming = np.random.choice([0, 1], size=self.max_count_costed_event-1, p=[0.7, 0.3])

        # Duration of call
        duration = []
        for i in range(len(event_type)):
            # if is CALL
            if event_type[i] == event_types[0]:
                duration.append(np.random.randint(1, 300))
            else: 
                duration.append(np.NaN)

        # Number_of_sms
        max_lenght_sms = 150                # Max simb in one sms
        simbols = np.random.normal(loc=90, scale=2.5, size=800)
        number_of_sms = []
        for i in range(self.max_count_costed_event - 1):
            rand_simb = np.random.choice(simbols)
            # If is SMS
            if event_type[i] == event_types[1]:
                if rand_simb > max_lenght_sms:
                    number_of_sms_el = int(rand_simb // 150 + 1)
                else:
                    number_of_sms_el = 1
                number_of_sms.append(number_of_sms_el)
            else:
                number_of_sms.append(np.NaN)

        # Date 
        date = []
        def random_date(start, end):
            """Generate a random datetime between `start` and `end`"""
            return start + datetime.timedelta(
                # Get a random amount of seconds between `start` and `end`
                seconds=random.randint(0, int((end - start).total_seconds())))

        d1 = datetime.datetime.strptime('01/01/2020 00:00 AM', '%m/%d/%Y %H:%M %p')
        d2 = datetime.datetime.strptime('12/31/2021 00:00 PM', '%m/%d/%Y %H:%M %p')
        for i in range(self.max_count_costed_event - 1):
            rnd = random_date(d1, d2)
            date.append(rnd.strftime("%Y-%m-%d %I:%M %p"))
        date.sort(key=lambda lst: (lst[8:10], lst[2:5], lst[0:2], lst[-2::1], lst[-8:-6], lst[-5:-3]))

        # Number of data (instead of total value)
        number_of_data = []
        for i in range(len(event_type)):
            # if is DATA
            if event_type[i] == event_types[2]:
                number_of_data.append(np.random.randint(500, 5000))
            else: 
                number_of_data.append(np.NaN)


        # Cost
        cost = []
        for i in range(self.max_count_costed_event - 1):

            pr_inst = product_instance_id_fk_from_ce[i] - 1

            # if is CALL
            if event_type[i] == event_types[0]:
                cost_el = productDF['cost_for_call'][pr_insDF['product_id_fk'][pr_inst]]
                cost.append(cost_el * (duration[i] // 60 + 1))    # per minute

            # if is SMS
            if event_type[i] == event_types[1]:
                cost_el = productDF['cost_for_sms'][pr_insDF['product_id_fk'][pr_inst]]
                cost.append(cost_el * number_of_sms[i])    # per sms

            # if is DATA
            if event_type[i] == event_types[2]:
                cost_el = productDF['cost_for_data'][pr_insDF['product_id_fk'][pr_inst]]
                cost.append(cost_el * (number_of_data[i]))    # per Mb


        Costed_eventDf = pd.DataFrame(
            {
                "event_id_pk": pd.Series(event_id_pk, name="event_id_pk", dtype="int"),
                "product_instance_id_fk": pd.Series(product_instance_id_fk_from_ce, name="product_instance_id_fk", dtype="int"),
                
                "calling_msisdn": pd.Series(calling_msisdn, name="calling_msisdn", dtype="int"),
                "called_msisdn": pd.Series(called_msisdn, name="called_msisdn", dtype="int"),

                "date_costed_event": pd.Series(date, name="date", dtype="str"),
                "cost": pd.Series(cost, name="cost", dtype="float"),
                "duration": pd.Series(duration, name="duration", dtype=pd.Int64Dtype()),
                "number_of_sms": pd.Series(number_of_sms, name="number_of_sms", dtype=pd.Int64Dtype()),
                "number_of_data": pd.Series(number_of_data, name="number_of_data", dtype=pd.Int64Dtype()),

                "event_type": pd.Series(event_type, name="event_type", dtype="str"),
                "direction": pd.Series(direction, name="direction", dtype="str"),
                "roaming": pd.Series(roaming, name="roaming", dtype="int"),
                
                # Add other columns here...
            }
        )
        Costed_eventDf.set_index('event_id_pk', inplace=True)
        return Costed_eventDf

    