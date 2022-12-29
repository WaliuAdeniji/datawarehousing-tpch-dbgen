![Tests](https://github.com/WaliuAdeniji/datawarehousing-tpch-dbgen/actions/workflows/tests.yaml/badge.svg)

This project is about creating a datawarehouse for TPC-H benchmark dataset. TPC-H is a standard dataset used as a benchmark for evaluating the performance of database management systems (DBMS). Using dimensional modeling, I did an ETL process on the data, stored it in BigQuery, and generated insight using SQL query. 

The capabilites served by subfolders are as follow:

|Subfolders       | Capabilities|
|:----------------|:------------|
|.github/workflows| CI/CD with Github Action|
|SQL-Analysis     | Images of SQL queries using BigQuery|
|Schedule-Query   | Automated query for data quality checks|
|iac              | Setup cloud resources using Workload Federated Authority and Terraform| 
|src              | ETL project setup for Airflow and Spark using Docker|
|tests            | DAG Integrity test for data pipeline and code formatting|

## ER Diagram
![ER Diagram](https://raw.githubusercontent.com/WaliuAdeniji/datawarehousing-tpch-dbgen/master/images/erd.png)

## Dimension Model
![Model](https://raw.githubusercontent.com/WaliuAdeniji/datawarehousing-tpch-dbgen/master/images/dim_model.png)
