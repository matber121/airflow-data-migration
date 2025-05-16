from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import logging
from config import *


def read_from_database(**kwargs):
    """
    Reads data from the database where 'exported_at' is set to a specific value and pushes the data to XCom.

    Args:
        **kwargs: Contextual keyword arguments from Airflow.
    """
    try:
        logging.info('Retrieving DB_CONN_STRING from Airflow Variables')
        db_conn_string = Variable.get("DB_CONN_STRING")
        
        logging.info('Creating connection with the database')
        engine = create_engine(db_conn_string)

        logging.info(f'Creating SQL query to read the data from {DB_SCHEMA}.{DB_TABLE} table')
        query = f"""
            SELECT * 
            FROM {DB_SCHEMA}.{DB_TABLE} 
            WHERE exported_at = '9999-12-31'
        """
        logging.info(f'Reading the data from the database using SQL: {query}')
        df = pd.read_sql(query, engine)
        logging.info('Successfully fetched the data.')

        logging.info('Converting the datetime fields to string')
        date_cols = df.select_dtypes(include=['datetime']).columns
        df[date_cols] = df[date_cols].astype(str)

        logging.info('Pushing records to XCom for use in the next task')
        kwargs['ti'].xcom_push(key='records', value=df.to_dict(orient='records'))

        logging.info('Closing connection')
        engine.dispose()
        logging.info('Connection closed')
    except Exception as e:
        logging.error(f"Error while reading records from table {DB_SCHEMA}.{DB_TABLE} in task read_from_database: {str(e)}")


def add_new_column(**kwargs):
    """
    Adds a new column 'product_id' to the records pulled from XCom and pushes the updated records back to XCom.

    Args:
        **kwargs: Contextual keyword arguments from Airflow.
    """
    try:
        logging.info('Pulling updated records from XCom')
        records = kwargs['ti'].xcom_pull(key='records', task_ids='reading_from_database')

        logging.info("Converting records to pandas DataFrame")
        df = pd.DataFrame(records)

        logging.info("Adding a new column called product_id with a constant value")
        df['product_id'] = 'ABCD'

        logging.info("Pushing updated records to XCom for use in the next task")
        kwargs['ti'].xcom_push(key='updated_records', value=df.to_dict(orient='records'))
    except Exception as e:
        logging.error(f"Error in add_new_column: {str(e)}")


def backup_table(**kwargs):
    """
    Backs up the specified table to a backup schema.

    Args:
        **kwargs: Contextual keyword arguments from Airflow.
    """
    try:
        logging.info('Retrieving DB_CONN_STRING from Airflow Variables')
        db_conn_string = Variable.get("DB_CONN_STRING")

        logging.info('Creating connection with the database')
        engine = create_engine(db_conn_string)

        with engine.connect() as connection:
            logging.info(f'Dropping {BACKUP_SCHEMA}.{DB_TABLE} if it exists')
            connection.execute(f"DROP TABLE IF EXISTS {BACKUP_SCHEMA}.{DB_TABLE}")

            logging.info(f'Creating backup table {BACKUP_SCHEMA}.{DB_TABLE}')
            query = f"CREATE TABLE {BACKUP_SCHEMA}.{DB_TABLE} AS SELECT * FROM {DB_SCHEMA}.{DB_TABLE};"
            connection.execute(query)

        logging.info(f"Backup table {BACKUP_SCHEMA}.{DB_TABLE} created successfully")
    except Exception as e:
        logging.error(f"Error while creating backup for {DB_TABLE}: {str(e)}")


def delete_records_from_table(**kwargs):
    """
    Deletes records from the specified table based on the 'base' column of the updated records.

    Args:
        **kwargs: Contextual keyword arguments from Airflow.
    """
    try:
        logging.info('Retrieving DB_CONN_STRING from Airflow Variables')
        db_conn_string = Variable.get("DB_CONN_STRING")

        logging.info('Creating connection with the database')
        engine = create_engine(db_conn_string)

        logging.info('Pulling updated records from XCom')
        updated_records = kwargs['ti'].xcom_pull(key='updated_records', task_ids='adding_row')

        if updated_records:
            logging.info("Extracting base from the updated records")
            base = [record['base'] for record in updated_records]

            logging.info(f"Executing query to delete records where base id is: {base}")
            with engine.connect() as connection:
                query = f"""
                    DELETE FROM {DB_SCHEMA}.{DB_TABLE}      
                    WHERE base IN ({','.join(f"'{bid}'" for bid in base)})
                """
                logging.info(query)
                connection.execute(query)

            logging.info(f"Deleted the records where base id is: {base}")
    except Exception as e:
        logging.error(f"Error while deleting records for task delete_records_from_table: {str(e)}")


def write_to_database(**kwargs):
    """
    Writes the updated records to the specified table in the database.

    Args:
        **kwargs: Contextual keyword arguments from Airflow.
    """
    try:
        logging.info('Retrieving DB_CONN_STRING from Airflow Variables')
        db_conn_string = Variable.get("DB_CONN_STRING")

        logging.info('Creating connection with the database')
        engine = create_engine(db_conn_string)

        logging.info('Pulling updated records from XCom')
        updated_records = kwargs['ti'].xcom_pull(key='updated_records', task_ids='adding_row')

        logging.info("Converting updated records to DataFrame")
        df = pd.DataFrame(updated_records)

        logging.info('Converting the datetime fields to string')
        for col in df.select_dtypes(include=['datetime']).columns:
            df[col] = df[col].astype(str)

        df.replace({'NaT': None}, inplace=True)
        logging.info('Dropping duplicate data from dataframe.')
        df.drop_duplicates(inplace=True)

        logging.info(f"Writing the updated records to the {DB_SCHEMA}.{DB_TABLE} table")
        df.to_sql(DB_TABLE, con=engine, schema=DB_SCHEMA, if_exists='append', index=False)

        logging.info(f"Updated records successfully inserted into the {DB_SCHEMA}.{DB_TABLE} table")
    except Exception as e:
        logging.error(f"Error while writing to {DB_TABLE} in task write_to_database: {str(e)}")


def update_exported_at(**kwargs):
    """
    Updates the 'exported_at' column for records in the specified table based on the 'message_id'.

    Args:
        **kwargs: Contextual keyword arguments from Airflow.
    """
    try:
        logging.info('Retrieving DB_CONN_STRING from Airflow Variables')
        db_conn_string = Variable.get("DB_CONN_STRING")

        logging.info('Creating connection with the database')
        engine = create_engine(db_conn_string)

        logging.info('Pulling updated records from XCom')
        updated_records = kwargs['ti'].xcom_pull(key='updated_records', task_ids='adding_row')

        if updated_records:
            logging.info("Extracting message_ids from the updated records")
            message_ids = [record['message_id'] for record in updated_records]

            logging.info(f"Updating exported_at column in the {DB_SCHEMA}.{DB_TABLE} table")
            with engine.connect() as connection:
                query = f"""
                    UPDATE {DB_SCHEMA}.{DB_TABLE}
                    SET exported_at = CURRENT_TIMESTAMP()
                    WHERE message_id IN ({','.join(f"'{mid}'" for mid in message_ids)})
                """
                connection.execute(query)

            logging.info(f"exported_at column updated for message_ids: {message_ids}")
    except Exception as e:
        logging.error(f"Error while updating the records for task update_exported_at: {str(e)}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 13),
    'retries': 0,
}

with DAG(
    'migrate_to_db',
    default_args=default_args,
    schedule_interval='*/5    ',  # Run every 5 minutes
    catchup=False,
) as dag:

    read_task = PythonOperator(
        task_id='reading_from_database',
        python_callable=read_from_database,
        provide_context=True,
    )

    add_row_task = PythonOperator(
        task_id='adding_row',
        python_callable=add_new_column,
        provide_context=True,
    )

    backup_task = PythonOperator(
        task_id='backup_table',
        python_callable=backup_table,
        provide_context=True,
    )

    delete_task = PythonOperator(
        task_id='delete_records_from_table',
        python_callable=delete_records_from_table,
        provide_context=True,
    )

    write_task = PythonOperator(
        task_id='writing_to_database',
        python_callable=write_to_database,
        provide_context=True,
    )

    update_exported_at_task = PythonOperator(
        task_id='updating_exported_at',
        python_callable=update_exported_at,
        provide_context=True,
    )

    read_task >> add_row_task >> backup_task >> delete_task >> write_task >> update_exported_at_task
