import sys
sys.path.append('/opt/airflow/Global_Electronics_Retailer')

from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime as dt
from ETL import Load, Clean, Combine, Analyze

default_args = {'owner':'Drishti',
                'start_date':dt.datetime(2015,7,8),
                'retries': 1,
                'retry_delay': dt.timedelta(minutes=10), 
               }

dag = DAG(dag_id = 'daily_etl_pipeline',
          description = 'ETL and retorting DAG for Global Electronics Retailer',
          default_args = default_args,
          schedule_interval = None, #None for manual triggers #'@daily' for Everyday at midnight
          catchup = False, #False: Don't fill missed run data #True:Runs all missed runs since start_date
          )

# ----------------------------- LOAD TASKS -----------------------------
Load_Customers = PythonOperator(task_id = 'load_customer',
                                python_callable = Load.load_customers,
                                dag=dag,
                               )

Load_Products = PythonOperator(task_id = 'load_products',
                               python_callable = Load.load_products,
                               dag=dag,
                              )

Load_Stores = PythonOperator(task_id = 'load_stores',
                             python_callable = Load.load_stores,
                             dag=dag,
                            )

Load_Sales = PythonOperator(task_id = 'load_sales',
                            python_callable = Load.load_sales,
                            dag=dag,
                           )

Load_Exchange_Rates = PythonOperator(task_id = 'load_exchange_rates',
                                     python_callable = Load.load_exchange_rates,
                                     dag=dag,
                                    )

# ----------------------------- CLEAN TASKS -----------------------------
Clean_Customers = PythonOperator(task_id='clean_customers',
                                 python_callable=Clean.Clean_Customer_Data,
                                 dag=dag,
                                )

Clean_Products = PythonOperator(task_id='clean_products',
                                python_callable=Clean.Clean_Product_Data,
                                dag=dag,
                               )

Clean_Stores = PythonOperator(task_id='clean_stores',
                              python_callable=Clean.Clean_Store_Data,
                              dag=dag,
                             )

Clean_Sales = PythonOperator(task_id='clean_sales',
                             python_callable=Clean.Clean_Sales_Data,
                             dag=dag,
                            )

Clean_Exchange_Rates = PythonOperator(task_id='clean_exchange_rates',
                                      python_callable=Clean.Clean_ExchangeRate_Data,
                                      dag=dag,
                                     )

# ----------------------------- COMBINE TASK -----------------------------
Combine_Data = PythonOperator(task_id='combine_data',
                               python_callable=Combine.Combine_Data,
                               dag=dag,
                             )

# ----------------------------- ANALYZE AND REPORT TASK -----------------------------
Generate_Report = PythonOperator(task_id='generate_report',
                                 python_callable=Analyze.analyze_and_report,
                                 dag=dag,
                                )

# ----------------------------- DEPENDENCIES -----------------------------
# 1. Load tasks run in parallel
# 2. Clean tasks run in parallel, each depends on corresponding load task
[Load_Customers, Load_Products, Load_Stores, Load_Sales, Load_Exchange_Rates] 

Load_Customers >> Clean_Customers 
Load_Products >> Clean_Products
Load_Stores >> Clean_Stores
Load_Sales >> Clean_Sales
Load_Exchange_Rates >> Clean_Exchange_Rates

# After all clean tasks complete, run combine
# 3. Combine_Data runs after all clean tasks
for task in [Clean_Customers, Clean_Products, Clean_Stores, Clean_Sales, Clean_Exchange_Rates]:
    task >> Combine_Data

# After combine finishes, run analyze and report task in parallel
Combine_Data >> Generate_Report


