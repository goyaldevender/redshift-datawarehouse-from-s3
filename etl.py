import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_data_into_staging(cursor, connection):
    """
    Loads data into staging tables using copy queries.

    Parameters:
    cursor: cursor object
        Cursor to execute SQL queries.
    connection: connection object
        Connection to the database.

    Returns:
    None
    """
    for sql_query in copy_table_queries:
        cursor.execute(sql_query)
        connection.commit()


def insert_data_into_tables(cursor, connection):
    """
    Inserts data into main tables from staging tables using insert queries.

    Parameters:
    cursor: cursor object
        Cursor to execute SQL queries.
    connection: connection object
        Connection to the database.

    Returns:
    None
    """
    for sql_query in insert_table_queries:
        cursor.execute(sql_query)
        connection.commit()


def run_etl_pipeline():
    """
    Runs the ETL pipeline, which includes loading data into the staging tables,
    and inserting data into main tables from the staging tables.

    Returns:
    None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_data_into_staging(cur, conn)
    insert_data_into_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    run_etl_pipeline()
