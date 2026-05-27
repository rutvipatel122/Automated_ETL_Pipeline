# Automated ETL Pipeline in Snowflake

An automated, end-to-end ETL (Extract, Transform, Load) pipeline built entirely within the Snowflake Data Cloud. This project demonstrates how to leverage Snowflake's native features to build a resilient, scalable, and automated data processing workflow.

## 🚀 Architecture & Key Features

* **Automated Orchestration:** Uses **Snowflake scheduled Tasks** to schedule and chain SQL execution dependencies without external orchestrators (like Airflow).
* **Change Data Capture (CDC):** Utilizes **Snowflake Streams** to track row-level changes (inserts, updates, deletes) on source tables for efficient incremental loading.
* **Modular Transformations:** Written in clean, optimized Snowflake SQL using Stored Procedures and Views to separate raw staging from analytical dimensions.
* **Error Handling & Logging:** Built-in mechanisms to track execution states and handle pipeline failures gracefully.

## 📁 Repository Structure

```text
├── sql, python scripts/
│   ├── Ingestion.py            #API call, Dataframe creation, Local system connecting, and loading data to Snowflake
│   ├── Raw_script.sql          # Target tables creation
│   ├── Staging_script.sql      # Landing tables, scheduled tasks, and error handling
│   └── Reporting.sql           # Regular and Materialized views, and testing
└── README.md
