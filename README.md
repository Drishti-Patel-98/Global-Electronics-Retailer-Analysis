## Project Overview:
Global Electronics Retailer Analysis project aims to deliver automatic business analytics solution that provides leadrship with timely, accurate, and actionable insights into sales, customer behavior, product performance, and operational efficiency across a multinational retail organization.

## Business Problem:
Business stakeholders currently rely on manual and fragmented reporting, resulting in:
- Delayed insights
- Inconsistent data interpretation
- Limited visibility into performance drivers
- Inefficient decision-making

There is no centralized reporting mechanism that integrates customer, sales, product, store, and exchange rate data.

## Business Objectives:
The objective of this project was to:
- Consolidate business data into a unified analytical dataset
- Deliver clear, visual insights for non-technical stakeholders
- Automate reporting to support recurring business reviews
- Enable data-driven decisions across sales, marketing, and operations

## Dataset overview:
The analysis uses five datasets:
| Dataset        | Purpose                            |
|:---------------|-----------------------------------:|
| Sales          | Revenue and volume analysis        |
| Customers      | Demographics and behavior insights |
| products       | Product category preformance       |
| Stores         | Regional comparision               |
| Exchange Rates | Currency normalization             |

## Diagram:
flowchart TD

A[Raw Data<br/>(CSV Files)<br/><br/>Customers.csv<br/>Products.csv<br/>Sales.csv<br/>Stores.csv<br/>Exchange_Rates.csv] 
--> |Daily| B[Data Ingestion & Validation<br/><br/>Load.py<br/>• Read CSV files<br/>• Validate schema & data types]

B --> |Daily| C[Data Cleaning & Preparation<br/><br/>Clean.py<br/>• Handle missing/null values<br/>• Standardize formats<br/>• Remove duplicates]

C --> |Daily| D[Data Integration & Modeling<br/><br/>Combine.py<br/>• Merge customers, sales, products<br/>• Join store & exchange rate data<br/>• Currency normalization<br/>• Derived business metrics]

D --> |Daily| E[Exploratory Data Analysis (EDA)<br/><br/>Analyze.py<br/>• Sales & revenue trends<br/>• Product & category performance<br/>• Store & regional insights<br/>• Customer behavior analysis<br/>• Delivery timeline metrics]

E --> |Daily| F[Automated Business Reporting<br/><br/>Global_Electronics_Retailer_Report.pdf<br/>• Charts<br/>• KPIs<br/>• Business insights]


## Flow Diagram:
┌─────────────────────────────────────────────┐
│                Raw Data                     │
|               (CSV Files)                   |
│                                             │
│  Customers.csv  Products.csv                │
│  Sales.csv      Stores.csv                  │
│  Exchange_Rates.csv                         │
└───────────────────────┬─────────────────────┘
                        │ Frequency: Daily
                        ▼
┌─────────────────────────────────────────────┐
│           Data Ingestion & Validation       │
│                                             │
│  Load.py                                    │
│  - Read CSV files                           │
│  - Validate schema & data types             │
└───────────────────────┬─────────────────────┘
                        │ Frequency: Daily
                        ▼
┌─────────────────────────────────────────────┐
│        Data Cleaning & Preparation          │
│                                             │
│  Clean.py                                   │
│  - Handle missing & null values             │
│  - Standardize data formats                 │
│  - Remove duplicates                        │
└───────────────────────┬─────────────────────┘
                        │ Frequency: Daily
                        ▼
┌─────────────────────────────────────────────┐
│           Data Integration & Modeling       │
│                                             │
│  Combine.py                                 │
│  - Merge customers, sales, products         │
│  - Join store & exchange rate data          │
│  - Currency normalization                   │
│  - Derived business metrics                 │
└───────────────────────┬─────────────────────┘
                        │ Frequency: Daily
                        ▼
┌─────────────────────────────────────────────┐
│        Exploratory Data Analysis (EDA)      │
│                                             │
│  Analyze.py                                 │
│  - Sales & revenue trends                   │
│  - Product & category performance           │
│  - Store & regional insights                │
│  - Customer behavior analysis               │
│  - Delivery timeline metrics                │
└───────────────────────┬─────────────────────┘
                        │ Frequency: Daily
                        ▼
┌─────────────────────────────────────────────┐
│          Automated Business Reporting       │
│                                             │
│  Global_Electronics_Retailer_Report.pdf     │
|  - PDF with charts and insighs              |
└───────────────────────┬─────────────────────┘

## Project Structure:
```
├── Data/
│   ├── Customers.csv
│   ├── Products.csv
│   ├── Sales.csv
│   ├── Stores.csv
│   └── Exchange_Rates.csv
├── ETL/
│   ├── tmp/    -- This folder contains all pickle files created during the ETL process
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

## Tech Stake:
Python (Pandas, NumPy, Matplotlib, Seaborn) | ETL | Apache Airflow

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
