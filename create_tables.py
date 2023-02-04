import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_existing_tables(cursor, connection):
    """
    Drops existing tables.

    Parameters:
    cursor: cursor object
        Cursor to execute SQL queries.
    connection: connection object
        Connection to the database.

    Returns:
    None
    """
    for sql_query in drop_table_queries:
        cursor.execute(sql_query)
        connection.commit()


def create_new_tables(cursor, connection):
    """
    Creates new tables.

    Parameters:
    cursor: cursor object
        Cursor to execute SQL queries.
    connection: connection object
        Connection to the database.

    Returns:
    None
    """
    for sql_query in create_table_queries:
        cursor.execute(sql_query)
        connection.commit()


def run_table_creation():
    """
    Runs the table creation process, which includes dropping existing tables and creating new tables.

    Returns:
    None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_existing_tables(cur, conn)
    create_new_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    run_table_creation()
