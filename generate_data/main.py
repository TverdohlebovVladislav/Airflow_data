
from data_generate_scripts import TableProductBase
# import _1_product as prod
from data_generate_scripts import customer_const
from data_generate_scripts import customer as cust
from data_generate_scripts import product_instance as prod_inst
from data_generate_scripts import costed_event as cost_ev
from data_generate_scripts import charge as charge
from data_generate_scripts import payment

def generate_data(cusmoers: int = 2, prof_inst: int = 4, cost_ev: int = 14, charge: int = 5, payment: int = 10) -> None:

    customer_const.castomer_const_find()

    TableProductBase.TableProductBase.max_count_costed_charge =charge
    TableProductBase.TableProductBase.max_count_costed_event = cost_ev
    TableProductBase.TableProductBase.max_count_costed_payment = payment
    TableProductBase.TableProductBase.max_count_customer = cusmoers
    TableProductBase.TableProductBase.max_count_product_inst = prof_inst

    # Don't change the order!
    cust.Customer().save_to_csv()
    prod_inst.ProductInstance().save_to_csv() 
    cost_ev.CostedEvent().save_to_csv() 
    charge.Charge().save_to_csv()
    payment.Payment().save_to_csv()

    
generate_data()