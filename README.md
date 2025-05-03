# ITSM_task

A complete data pipeline for IT Service Management (ITSM) analytics using Apache Airflow, DBT, PostgreSQL, and Apache Superset. This project processes, transforms, and visualizes ServiceNow ticket data to generate actionable service delivery insights.

---

## ITSM Data Pipeline Project

This project showcases a complete end-to-end data pipeline for processing, transforming, and visualizing IT Service Management (ITSM) data using the modern data stack: **Apache Airflow**, **DBT**, **PostgreSQL**, and **Apache Superset**.

The goal is to derive actionable insights from a ServiceNow ticket dump by building a modular, automated analytics pipeline.

---

## Tech Stack

- **Apache Airflow**: For orchestrating the pipeline (data ingestion + transformations)
- **DBT (Data Build Tool)**: For data transformation and modeling
- **PostgreSQL**: Relational database to store raw and processed data
- **Apache Superset**: Dashboard and visualization layer
- **Python + Pandas**: For data loading and manipulation

---


## Setup & Execution

### 1. Environment Setup

Two virtual environments were used for this project:
- **Airflow Environment**: For running DAGs and managing data ingestion + scheduling
- **DBT Environment**: For managing transformations and DBT runs

All necessary packages are installed via `pip` (Airflow, DBT, pandas, openpyxl, etc.).

### 2. Data Ingestion (Airflow)

- A DAG named `servicenow_analytics_pipeline` is created.
- Reads `data.xlsx` (ServiceNow ticket dump) from the desktop.
- Ingests it into a PostgreSQL table named `servicenow_tickets_raw`.

### 3. DBT Transformations

6 DBT models are implemented to transform and aggregate the data:

- `servicenow_tickets_cleaned.sql`: Removes duplicates, handles nulls, and standardizes formats.
- `category_wise_tickets.sql`: Aggregates ticket counts by category.
- `servicenow_tickets_date_extracted.sql`: Extracts Year, Month, Day from `Created Date`.
- `servicenow_tickets_avg_resolution_time.sql`: Calculates average resolution time by Category and Priority.
- `servicenow_tickets_closure_rate.sql`: Calculates closure rate by Assigned Group.
- `servicenow_monthly_ticket_summary.sql`: Monthly aggregation of ticket count, resolution time, and closure rate.

### 4. Workflow Scheduling

- The Airflow DAG is scheduled to run **daily** using:
  ```python
  schedule_interval='@daily'
