import numpy as np
import pandas as pd
import sys
sys.path.append('/opt/airflow/Global_Electronics_Retailer')

def Combine_Data(Customers=None, Products=None, Stores=None, Sales=None, Exchange_Rates=None,
                 customers_path='/opt/airflow/Global_Electronics_Retailer/ETL/tmp/customers_cleaned.pkl',
                 products_path='/opt/airflow/Global_Electronics_Retailer/ETL/tmp/products_cleaned.pkl',
                 stores_path='/opt/airflow/Global_Electronics_Retailer/ETL/tmp/stores_cleaned.pkl',
                 sales_path='/opt/airflow/Global_Electronics_Retailer/ETL/tmp/sales_cleaned.pkl',
                 exchange_rates_path='/opt/airflow/Global_Electronics_Retailer/ETL/tmp/exchange_rates_cleaned.pkl',
                 output_path='/opt/airflow/Global_Electronics_Retailer/ETL/tmp/combined_data.pkl',
                 return_df=False):
    
    # Load from pickle if dataframe not provided
    if Customers is None:
        Customers = pd.read_pickle(customers_path)

    if Products is None:
        Products = pd.read_pickle(products_path)

    if Stores is None:
        Stores = pd.read_pickle(stores_path)

    if Sales is None:
        Sales = pd.read_pickle(sales_path)

    if Exchange_Rates is None:
        Exchange_Rates = pd.read_pickle(exchange_rates_path)

    #Merge Sales with Product on ProductKey
    Sales_Products = pd.merge(Sales,Products,on='ProductKey',how='inner')
    
    #Merge Customers on CustomerKey
    Sales_Products_Customers = pd.merge(Sales_Products,Customers,on='CustomerKey',how='inner')

    #Merge Stores on StoreKey
    Full_Data = pd.merge(Sales_Products_Customers,Stores,on='StoreKey',how='inner')

    #Calculate Sales Amount, Cost and Profit
    Full_Data['Sales Amount USD'] = Full_Data['Quantity'] * Full_Data['Unit Price USD']
    Full_Data['Cost'] = Full_Data['Quantity'] * Full_Data['Unit Cost USD']
    Full_Data['Profit'] = Full_Data['Sales Amount USD'] - Full_Data['Cost']
    
    # Calculate Profit Margin safely (avoid division by zero)
    Full_Data['Profit_Margin'] = np.where(Full_Data['Sales Amount USD'] != 0,
                                          Full_Data['Profit'] / Full_Data['Sales Amount USD'],
                                          0
                                         )
    
    if output_path:
        Full_Data.to_pickle(output_path)

    if return_df:
        return Full_Data