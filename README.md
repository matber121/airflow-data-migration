Project Objective
This project implements an automated ETL (Extract-Transform-Load) pipeline using Apache Airflow to:

Extract data from a staging table in Snowflake where records are marked as not yet exported.

Transform data by adding a required product_id field.

Backup the staging table for data lineage and audit.

Load the transformed data into the destination table by:

Deleting old versions of matching records using a natural key.

Inserting new records.

Mark records as exported in the staging table by updating the EXPORTED_AT column.

This pipeline is designed to run periodically (e.g., daily/weekly) or triggered manually to refresh the production table using only the latest records from the staging environment.

🗂️ Project Structure
graphql
Copy
Edit
airflow_dags/
│
├── dags/
│   └── migrate_to_db.py        # Airflow DAG with full pipeline orchestration
│
├── plugins/
│   └── helpers.py              # Helper methods for Snowflake connection handling
│
├── config.py                   # Schema names, table names, and reusable SQL templates
│
└── README.md                   # This file
⚙️ Components Breakdown
📄 config.py
This module holds all configurable metadata and SQL query templates, including:

Schema and Table names

SQL templates for:

Column mapping

Reading data

Backup creation

Deletion

Updates

python
Copy
Edit
MYSQL_SCHEMA = 'stg'
MYSQL_TABLE = 'map'

DB_SCHEMA = 'STG'
DB_TABLE = 'LPS_MAP'
DB_SRC_SCHEMA = 'STG'
DB_SRC_TABLE = 'LPS_MAP'
SQL templates are parameterized and dynamically rendered within the DAG.

🧠 helpers.py
Contains two key functions:

get_snowflake_conn_string():
Retrieves Snowflake credentials from Airflow Connections (SNOWFLAKE_CONN_) using BaseHook.

create_snowflake_connection(conn_details):
Constructs a Snowflake SQLAlchemy engine using the credentials retrieved above.

📊 DAG Flow: migrate_to_db.py
This DAG is composed of the following tasks:

Task ID	Description
reading_from_database	Query staging table (STG.LPS_MAP) for rows with EXPORTED_AT = '9999-12-31'
adding_row	Append product_id = 'ABCD' to the dataset
backup_table	Backup the staging table to a backup schema
delete_records_from_table	Delete matching rows from target table using natural key
writing_to_database	Insert the transformed data into the destination table
updating_exported_at	Update the EXPORTED_AT timestamp for source records

🔁 Pipeline Diagram
mermaid
Copy
Edit
graph TD

A[🔍 Read staging data<br>(EXPORTED_AT='9999-12-31')] --> B[➕ Add column<br>'product_id']
B --> C[💾 Backup staging table<br>to backup schema]
C --> D[🗑️ Delete matching rows<br>in destination table]
D --> E[📥 Insert new records<br>to target table]
E --> F[🕓 Update 'EXPORTED_AT'<br>in staging table]
🔐 Airflow Connection Configuration
Ensure the following Airflow connection exists in the UI under Admin → Connections:

Conn ID: SNOWFLAKE_CONN_

Conn Type: Snowflake

Login: your_username

Password: your_password

Account: xy12345.region

Schema: your_default_db

Extra (JSON format):
