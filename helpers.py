#helpers.py

from airflow.hooks.base import BaseHook
import logging
from sqlalchemy import create_engine


def create_snowflake_connection(conn_details):
    """
    Creates a SQLAlchemy engine for Snowflake using the provided connection details.

    Args:
        conn_details (dict): A dictionary containing the Snowflake connection details.

    Returns:
        sqlalchemy.engine.base.Engine: A SQLAlchemy engine connected to Snowflake.
    """
    try:
        logging.info('Creating Snowflake connection string for SQLAlchemy')

        # Create the Snowflake SQLAlchemy connection string
        conn_string = (
            f"snowflake://{conn_details['user']}:{conn_details['password']}@{conn_details['account']}/"
            f"{conn_details['database']}/?warehouse={conn_details['warehouse']}"
            f"&role={conn_details['role']}"
        )

        logging.info('Initializing SQLAlchemy engine for Snowflake')
        # Create the SQLAlchemy engine
        engine = create_engine(conn_string)

        logging.info('Successfully created SQLAlchemy engine')
        return engine
    except Exception as e:
        logging.error(f"Error creating SQLAlchemy engine: {str(e)}")
        raise ValueError("Failed to create Snowflake connection using SQLAlchemy.")


def get_snowflake_conn_string():
    """
    Retrieves the Snowflake connection string from the Airflow Connections.
    
    Returns:
        dict: The Snowflake connection details including login credentials.
    """
    try:
        logging.info('Retrieving Snowflake connection from Airflow Connections')
        conn = BaseHook.get_connection("SNOWFLAKE_CONN_")

        # Create the connection string from the connection details
        snowflake_conn_string = {
            'account': conn.extra_dejson.get('account', ''),
            'user': conn.login,
            'password': conn.password,
            'warehouse': conn.extra_dejson.get('warehouse', ''),
            'database': conn.schema,
            'role': conn.extra_dejson.get('role', '')
        }

        logging.info('Successfully retrieved Snowflake connection string')
        return snowflake_conn_string
    except Exception as e:
        logging.error(f"Error retrieving Snowflake connection: {str(e)}")
        raise ValueError("Could not retrieve the Snowflake connection.")
