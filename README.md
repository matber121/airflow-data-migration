Airflow DAG Overview: migrate_to_db
This DAG orchestrates the migration of records from a source database table by performing the following sequence of operations every 5 minutes:

Environment Configuration
Airflow Variable Required:

DB_CONN_STRING: Connection string for SQLAlchemy to connect to the database.

Imported Constants:

DB_SCHEMA, DB_TABLE, BACKUP_SCHEMA are imported from config.py.

Task Breakdown
🧪 reading_from_database
Purpose: Extracts records where exported_at = '9999-12-31'.

Steps:

Connects to the database.

Reads matching records from {DB_SCHEMA}.{DB_TABLE}.

Converts datetime fields to string.

Pushes records to XCom under records.

🧩 adding_row
Purpose: Adds a new column product_id with a constant value 'ABCD'.

Steps:

Pulls records from XCom.

Converts to DataFrame and adds the new column.

Pushes the updated records to XCom under updated_records.

💾 backup_table
Purpose: Creates a full backup of the target table in the backup schema.

Steps:

Drops the existing backup table if it exists.

Recreates it with all data from {DB_SCHEMA}.{DB_TABLE} in {BACKUP_SCHEMA}.

🗑️ delete_records_from_table
Purpose: Deletes rows from the original table based on base values in the updated records.

Steps:

Pulls updated_records from XCom.

Extracts base values.

Deletes rows in {DB_SCHEMA}.{DB_TABLE} where base matches any of these values.

📝 writing_to_database
Purpose: Appends the modified records back into the table.

Steps:

Pulls updated_records from XCom.

Cleans DataFrame: converts datetime, drops duplicates, replaces NaT.

Appends data to {DB_SCHEMA}.{DB_TABLE}.

🕓 updating_exported_at
Purpose: Updates the exported_at timestamp to current time for modified records.

Steps:

Pulls updated_records from XCom.

Updates exported_at for rows matching message_id.

DAG Flow:
text
Copy
Edit
reading_from_database
          ↓
     adding_row
          ↓
     backup_table
          ↓
delete_records_from_table
          ↓
   writing_to_database
          ↓
  updating_exported_at
