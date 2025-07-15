import pandas as pd
import numpy as np
import sys
sys.path.append('/opt/airflow/Global_Electronics_Retailer')

#------------------------------------------------------------------------------------------------------------------------------------------------------- 
def Clean_Customer_Data(Customers = None, input_path = '/opt/airflow/Global_Electronics_Retailer/ETL/tmp/customers.pkl', output_path = '/opt/airflow/Global_Electronics_Retailer/ETL/tmp/customers_cleaned.pkl', return_df = False):
    '''
    Args:
        Customers (pd.DataFrame, optional): If provided, use this dataframe instead of loading from input_path.
        input_path (str, optional): Path to input pickle file. Used only if Customers dataframe is None.
        output_path (str, optional): Path to save cleaned pickle file. Saves only if provided.
        return_df (bool, optional): If True, returns the cleaned dataframe.

    Returns:
        pd.DataFrame or None: Cleaned dataframe if return_df=True, else None.
    
    Reason to keep both dataframe and pickle file path:
        Dataframe: If we want to test and debug quickly
        Pickle file path: For airflow data movement
    '''

    if Customers is None:
        Customers = pd.read_pickle(input_path)

    #Drop duplicates keeping only the first occurrence
    Customers = Customers.drop_duplicates()

    #Rename columns
    Customers = Customers.rename(columns={'Name': 'Customer Name', 
                                          'City': 'Customer City',
                                          'State Code':'Customer State Code',
                                          'State':'Customer State',
                                          'Zip Code':'Customer Zip Code',
                                          'Country':'Customer Country',
                                          'Continent':'Customer Continent'
                                         }
                                )
    #Convert datatype of 'Birthday' from string to Date
    Customers['Birthday'] = pd.to_datetime(Customers['Birthday'],format='%m/%d/%Y', errors='coerce')
    
    #Replace null with NA(Napoli)
    Customers.loc[Customers['Customer State'] == 'Napoli', 'Customer State Code'] = 'NA'
    
    # Check for nulls and show warning
    null_customer_summary = Customers.isnull().sum()
    null_customer_columns = null_customer_summary[null_customer_summary > 0]
    
    if not null_customer_columns.empty:
        print("Warning: The following columns have missing values:")
        print(null_customer_columns)
    else:
        print("No missing values found in Customers data.")
    
    if output_path:
        Customers.to_pickle(output_path)

    if return_df:
        return Customers

#-------------------------------------------------------------------------------------------------------------------------------------------------------
def Clean_Product_Data(Products = None, input_path = '/opt/airflow/Global_Electronics_Retailer/ETL/tmp/products.pkl', output_path = '/opt/airflow/Global_Electronics_Retailer/ETL/tmp/products_cleaned.pkl', return_df = False):

    if Products is None:
        Products = pd.read_pickle(input_path)

    #Drop duplicates keeping only the first occurrence
    Products = Products.drop_duplicates()
    
    #Convert datatype of 'Unit Cost USD' from string to float
    Products['Unit Cost USD'] = (Products['Unit Cost USD']
                                 .astype(str)                              # Convert everything to string
                                 .str.replace('[$,]', '', regex=True)      # Remove $ and commas
                                 .replace({'nan': None, '': None})         # Optional: Handle empty strings
                                 .astype(float)                            # Convert to float
                                 .fillna(0)                                # fill missing cost with 0
                                )
    
    #Convert datatype of 'Unit Price USD' from string to float
    Products['Unit Price USD'] = (Products['Unit Price USD']
                                  .astype(str)                              # Convert everything to string
                                  .str.replace('[$,]', '', regex=True)      # Remove $ and commas
                                  .replace({'nan': None, '': None})         # Optional: Handle empty strings
                                  .astype(float)                            # Convert to float
                                  .fillna(0)                                # fill missing price with 0
                                 )
    
    # Check for nulls and show warning
    null_product_summary = Products.isnull().sum()
    null_product_columns = null_product_summary[null_product_summary > 0]
    
    if not null_product_columns.empty:
        print("Warning: The following columns in Products have missing values:")
        print(null_product_columns)
    else:
        print("No missing values found in Products data.")

    if output_path:
        Products.to_pickle(output_path)

    if return_df:
        return Products

#-------------------------------------------------------------------------------------------------------------------------------------------------------
def Clean_Store_Data(Stores = None, input_path = '/opt/airflow/Global_Electronics_Retailer/ETL/tmp/stores.pkl', output_path = '/opt/airflow/Global_Electronics_Retailer/ETL/tmp/stores_cleaned.pkl', return_df = False):

    if Stores is None:
        Stores = pd.read_pickle(input_path)

    #Drop duplicates keeping only the first occurrence
    Stores = Stores.drop_duplicates()
    
    #Convert datatype of 'Open Date' from string to Date
    Stores['Open Date'] = pd.to_datetime(Stores['Open Date'],format='%m/%d/%Y', errors='coerce')
    
    #Rename columns
    Stores = Stores.rename(columns={'Country': 'Store Country', 
                                    'State': 'Store State',
                                    'Open Date':'Store Open Date'
                                   }
                          )
    
    # Check for nulls and show warning
    null_store_summary = Stores.isnull().sum()
    null_store_columns = null_store_summary[null_store_summary > 0]

    if not null_store_columns.empty:
        print("Warning: The following columns in Stores have missing values:")
        print(null_store_columns)
    else:
        print("No missing values found in Stores data.")
    
    if output_path:
        Stores.to_pickle(output_path)

    if return_df:
        return Stores

#-------------------------------------------------------------------------------------------------------------------------------------------------------
def Clean_ExchangeRate_Data(Exchange_Rates = None, input_path = '/opt/airflow/Global_Electronics_Retailer/ETL/tmp/exchange_rates.pkl', output_path = '/opt/airflow/Global_Electronics_Retailer/ETL/tmp/exchange_rates_cleaned.pkl', return_df = False):

    if Exchange_Rates is None:
        Exchange_Rates = pd.read_pickle(input_path)

    #Drop duplicates keeping only the first occurrence
    Exchange_Rates = Exchange_Rates.drop_duplicates()
    
    #Convert datatype of 'Date' from string to Date
    Exchange_Rates['Date'] = pd.to_datetime(Exchange_Rates['Date'],format='%m/%d/%Y', errors='coerce')

    # Check for nulls and show warning
    null_exchange_rate_summary = Exchange_Rates.isnull().sum()
    null_exchange_rate_columns = null_exchange_rate_summary[null_exchange_rate_summary > 0]

    if not null_exchange_rate_columns.empty:
        print("Warning: The following columns in Exchange_Rates have missing values:")
        print(null_exchange_rate_columns)
    else:
        print("No missing values found in Exchange_Rates data.")
    
    if output_path:
        Exchange_Rates.to_pickle(output_path)

    if return_df:
        return Exchange_Rates

#-------------------------------------------------------------------------------------------------------------------------------------------------------
def Clean_Sales_Data(Sales = None, input_path = '/opt/airflow/Global_Electronics_Retailer/ETL/tmp/sales.pkl', output_path = '/opt/airflow/Global_Electronics_Retailer/ETL/tmp/sales_cleaned.pkl', return_df = False):

    if Sales is None:
        Sales = pd.read_pickle(input_path)

    #Drop duplicates keeping only the first occurrence
    Sales = Sales.drop_duplicates()
    
    #Convert datatype of 'Order Date' from string to Date
    Sales['Order Date'] = pd.to_datetime(Sales['Order Date'],format='%m/%d/%Y', errors='coerce')
    
    #Convert datatype of 'Delivery Date' from string to Date
    Sales['Delivery Date'] = pd.to_datetime(Sales['Delivery Date'],format='%m/%d/%Y', errors='coerce')

    # Fill missing Quantity with 0 to avoid errors later
    Sales['Quantity'] = Sales['Quantity'].fillna(0)

    # Check for nulls and show warning
    null_sales_summary = Sales.isnull().sum()
    null_sales_columns = null_sales_summary[null_sales_summary > 0]
    
    if not null_sales_columns.empty:
        print("Warning: The following columns in Sales have missing values:")
        print(null_sales_columns)
    else:
        print("No missing values found in Sales data.")
    
    if output_path:
        Sales.to_pickle(output_path)

    if return_df:
        return Sales
