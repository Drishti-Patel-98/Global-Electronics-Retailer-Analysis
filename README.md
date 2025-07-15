## Project Overview:
Global Electronics Retailer Analysis is a Python-based data pipeline designed to clean, integrate, analyze, and visualize data from a multinational electronics retail business. The final output is an automated PDF report (Global_Electronics_Retailer_Report.pdf) that includes visualizations for decision-makers.

## Project Structure:
```
├── Data/
│   ├── Customers.csv
│   ├── Products.csv
│   ├── Sales.csv
│   ├── Stores.csv
│   └── Exchange_Rates.csv
├── ETL/
│   ├── tmp/    -- This folder contains all pickle files created during the CTL process
│   ├── Load.py
│   ├── Clean.py
│   ├── Combine.py
│   ├── Analyze.py
├── Output/
|   ├── Global_Electronics_Retailer_Report.pdf
├── dags/
|   ├── ETL_DAG.py
├── Global Electronics Retailer Analysis.py -- This script is no longer in use as modularized scripts are available in the ETL folder
├── Insights and Recommendations.pdf
├── Requirements.pdf
└── README.md
```

## Key Features:
- Data Cleaning: Handles inconsistent data types, missing values, and nulls.
- Data Merging: Combines customer, sales, product, store, and exchange rate data.
- Exploratory Analysis: Visualizes sales trends, product performance, store metrics, customer demographics, and delivery timelines.
- PDF Report Generation: Automatically generates a well-formatted report with charts and section titles.
- Daily scheduling using Apache Airflow.

## Output:
[View Output](Output/Global_Electronics_Retailer_Report.pdf)


## Insights and Recommendations:
[View Insights and Recommendations](Insights%20and%20Recommendations.pdf)
