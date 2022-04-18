import numpy as np
import pandas as pd
import random
from datetime import datetime as DT
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse as du_parse
from data_generate_scripts.TableProductBase import TableProductBase

class ProductInstance(TableProductBase):

    def __init__(self):
        self.dataFrame = self.get_df()
        self.save_csv = self.save_to_csv()

    def get_df(self) -> pd.DataFrame:
        max_count_product = TableProductBase.get_max_count_product()
        product_data = TableProductBase.get_df()

        cust = pd.read_csv("data_source/customer_const.csv")
        cust.set_index('customer_id', inplace=True)


        #generate ban-dict for customer_id who has inactive status 
        #and for customer_id there is already one of the active elements from dict (termination_date == None)
        ban = dict(tariff= dict(cust_id = [],date = []) , video= dict(cust_id = [],date = []) ,music = dict(cust_id = [],date = []))

        def rand_date(date_start,date_end):
            delta = date_end - date_start
            return date_start + timedelta(random.randint(0, delta.days))  

        start_dt = DT.strptime('01.01.2020', '%d.%m.%Y')
        end_dt = DT.strptime('31.12.2021', '%d.%m.%Y')

        cust_status = cust['status']
        cust_birth = cust['data_of_birth']

        flag = []
        for i in cust_status.index:
            if cust_status.values[i-1] =='inactive':
                date = rand_date(start_dt, end_dt)
                flag.append(i)
                for key in ban:
                    ban.setdefault(key).setdefault('cust_id').append(i) 
                    ban.setdefault(key).setdefault('date').append(date)

        base_of_date = []   
        termin_date = [] 
        #generate some fields
        product_instance_id_PK: list  = [i for i in range(1, self.max_count_product_inst+1)]
        customer_id_FK: list  = np.random.randint(1, self.max_count_customer, size=self.max_count_product_inst)


        #create list with index ​​for tariffs that cannot be connected twice at the same time at the customer
        product_only_once = dict(tariff= list(product_data[product_data.product_type == 'Tariff'].index) , video= list(product_data[product_data.product_category == 'Video'].index) ,music = list(product_data[product_data.product_category == 'Music'].index))

        #create list with index ​​for tariffs that can be connected an unlimited number of times at the same time at the bustomer
        many = list(product_data[product_data.product_type == 'addon'].index)

        product_id_FK : list = []

        #generate base of date , termination date , product_id_FK
        #taking into account the dependencies of finding customer_id in the ban list
        #also taking into account dependencies on customer_id status
        #and consistency of date generation depending on the next values ​​for each customer_id
        for i in customer_id_FK:
            a = np.random.randint(1, max_count_product)
            if i not in ban['tariff']['cust_id']:
                product_id_FK.append(np.random.choice(product_only_once['tariff']))
                base_of_date.append(rand_date(start_dt, end_dt))
                termin_date.append(np.NaN)   
                ban.setdefault('tariff').setdefault('cust_id').append(i) 
                ban.setdefault('tariff').setdefault('date').append(base_of_date[-1]) 
            elif a in many:
                if i in flag:      
                    t = ban['tariff']['date'][ban['tariff']['cust_id'].index(i)]
                    t1 = rand_date(t - relativedelta(years=2), t - relativedelta(months=2))
                    t2 = t1 + relativedelta(months=1)
                    t3 = t1 + relativedelta(months=2) 
                    product_id_FK.append(a)
                    base_of_date.append(t1)
                    termin_date.append(np.random.choice([t2,t3],p = [0.57,0.43]))      
                else:
                    t1 = rand_date(start_dt, end_dt - relativedelta(months=2))
                    t2 = t1 + relativedelta(months=1)
                    t3 = t1 + relativedelta(months=2)   
                    product_id_FK.append(a)
                    base_of_date.append(t1)
                    termin_date.append(np.random.choice([np.NaN,t2,t3],p = [0.35,0.45,0.2])) 
            else:    
                for key in ban:
                    if i not in ban[key]['cust_id'] and a in product_only_once[key]:
                        product_id_FK.append(a)
                        base_of_date.append(rand_date(start_dt, end_dt))        
                        ban.setdefault(key).setdefault('cust_id').append(i) 
                        ban.setdefault(key).setdefault('date').append(base_of_date[-1]) 
                        termin_date.append(np.NaN)      
                        break
                    elif i in ban[key]['cust_id'] and a in product_only_once[key]:
                        t = ban[key]['date'][ban[key]['cust_id'].index(i)]
                        t1 = rand_date(start_dt, t)
                        t2 = rand_date(start_dt, t)
                        if (t1>t2): t1,t2 = t2,t1 
                        product_id_FK.append(a)
                        ban[key]['date'][ban[key]['cust_id'].index(i)] = t1
                        base_of_date.append(t1)
                        termin_date.append(t2)       
                        break

         #get status      
        status = []     
        for i in termin_date: 
            if pd.isna(i):status.append(1)
            else: status.append(0)

        #get distribution channel
        distirb_chanel = []
        d1 = du_parse(str(DT.strptime('01.01.2022', '%d.%m.%Y') ))
        for j in customer_id_FK:
            d2 = du_parse(str(cust_birth[j]))
            delta = relativedelta(d1, d2).years
            if cust['autopay_card'][j] == 'Yes': pay = ['Web site','Mobile app','Physical shop','Call center']
            else: pay = ['Physical shop','Call center','Physical shop','Call center']  
            if delta<26:
                distirb_chanel.append(np.random.choice(pay, p = [0.25,0.7,0.04,0.01]))
            if delta > 25 and delta < 36:
                distirb_chanel.append(np.random.choice(pay, p = [0.45,0.45,0.05,0.05]))
            if delta > 35 and delta < 46:
                distirb_chanel.append(np.random.choice(pay, p = [0.5,0.3,0.1,0.1]))   
            if delta > 45 and delta < 56:
                distirb_chanel.append(np.random.choice(pay, p = [0.59,0.31,0.1,0]))      
            if delta>55:
                distirb_chanel.append(np.random.choice(pay, p = [0.7,0.1,0.1,0.1]))

        Product_instanceDf = pd.DataFrame(
            {
                "product_instance_id_PK": pd.Series(product_instance_id_PK, name="product_instance_id_PK", dtype="int"),
                "customer_id_FK": pd.Series(customer_id_FK, name="customer_id_FK", dtype="int"),
                "product_id_FK": pd.Series(product_id_FK, name="product_id_FK", dtype="int"),
                "activation_date":  pd.Series(base_of_date,name = "activation_date", dtype = "str"),
                "termination_date": pd.Series(termin_date,name = "activation_date", dtype = "str"),
                "status": pd.Series(status,name = "status", dtype = "str"),
                "distribution_channel" : pd.Series(distirb_chanel,name = "distribution_channel", dtype = "str")
            }
        )
        Product_instanceDf['activation_date'] = pd.to_datetime(Product_instanceDf['activation_date'], format='%Y-%m-%dT')
        Product_instanceDf['termination_date'] = pd.to_datetime(Product_instanceDf['termination_date'], format='%Y-%m-%dT')
        Product_instanceDf.set_index('product_instance_id_PK', inplace=True)
        return Product_instanceDf
#a = ProductInstance()
#a.save_csv

