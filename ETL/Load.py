import pandas as pd
import sys
sys.path.append('/opt/airflow/Global_Electronics_Retailer')

'''
What is Pickle?
    - Pickle is a Python module that lets you save any Python object (like a DataFrame) to a file on disk.
    - Later, you can load that file back and get the exact same object.
Why use Pickle here?
    - Airflow tasks run separately.
    - Can’t just pass a DataFrame directly between tasks.
    - So, need to save the DataFrame to a file (pickle file).
    - The next task loads the DataFrame from that file and continues.
'''

def load_customers():
    Customers = pd.read_csv('/opt/airflow/Global_Electronics_Retailer/data/Customers.csv', encoding='unicode_escape')
    Customers.to_pickle('/opt/airflow/Global_Electronics_Retailer/ETL/tmp/customers.pkl')  # save pickle
    #return Customers #Uncomment when debugging locally

def load_products():
    Products = pd.read_csv('/opt/airflow/Global_Electronics_Retailer/data/Products.csv', encoding='unicode_escape')
    Products.to_pickle('/opt/airflow/Global_Electronics_Retailer/ETL/tmp/products.pkl')
    #return Products #Uncomment when debugging locally

def load_stores():
    Stores = pd.read_csv('/opt/airflow/Global_Electronics_Retailer/data/Stores.csv', encoding='unicode_escape')
    Stores.to_pickle('/opt/airflow/Global_Electronics_Retailer/ETL/tmp/stores.pkl')
    #return Stores #Uncomment when debugging locally

def load_sales():
    Sales = pd.read_csv('/opt/airflow/Global_Electronics_Retailer/data/Sales.csv', encoding='unicode_escape')
    Sales.to_pickle('/opt/airflow/Global_Electronics_Retailer/ETL/tmp/sales.pkl')
    #return Sales #Uncomment when debugging locally

def load_exchange_rates():
    Exchange_Rates = pd.read_csv('/opt/airflow/Global_Electronics_Retailer/data/Exchange_Rates.csv', encoding='unicode_escape')
    Exchange_Rates.to_pickle('/opt/airflow/Global_Electronics_Retailer/ETL/tmp/exchange_rates.pkl')
    #return Exchange_Rates #Uncomment when debugging locally
