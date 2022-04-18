
# from data_generate_scripts import TableProductBase
# import _1_product as prod
from data.data_generate_scripts import customer_const
from data.data_generate_scripts import customer as cust
from data.data_generate_scripts import product_instance as prod_inst
from data.data_generate_scripts import costed_event as cost_ev
from data.data_generate_scripts import charge as charge
from data.data_generate_scripts import payment

# def generate_data(cusmoers: int = 2, prof_inst: int = 4, cost_ev_c: int = 14, charge_c: int = 5, payment_c: int = 10) -> None:

#     customer_const.castomer_const_find()

#     TableProductBase.TableProductBase.max_count_costed_charge = charge_c
#     TableProductBase.TableProductBase.max_count_costed_event = cost_ev_c
#     TableProductBase.TableProductBase.max_count_costed_payment = payment_c
#     TableProductBase.TableProductBase.max_count_customer = cusmoers
#     TableProductBase.TableProductBase.max_count_product_inst = prof_inst

#     # Don't change the order!
#     cust.Customer().add_data_to_csv()
#     prod_inst.ProductInstance().add_data_to_csv() 
#     cost_ev.CostedEvent().add_data_to_csv() 
#     charge.Charge().add_data_to_csv()
#     payment.Payment().add_data_to_csv()


def generate_data():

    customer_const.castomer_const_find()

    # Don't change the order!
    cust.Customer().save_to_csv()
    prod_inst.ProductInstance().save_to_csv() 
    cost_ev.CostedEvent().save_to_csv() 
    charge.Charge().save_to_csv()
    payment.Payment().save_to_csv()
    