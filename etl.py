import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from Amazon S3 into staging tables in Amazon Redshift.

    This function iterates through a list of SQL queries specified in the 'copy_table_queries' list
    and executes each query. Staging_events_copy is responsible for copying data from S3
    into staging_event table while staging_songs_copy handle the staging_song table. 

    Parameters:
    cur (psycopg2.cursor): The database cursor used to execute SQL queries.
    conn (psycopg2.connect): The database connection object.

    Returns:
    None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Executes insert queries from insert_table_queries list to transfer data 
    from staging tables to fact and dimension tables.

    Parameters:
    cur (cursor object): Cursor for database operations.
    conn (connection object): Connection to the database.

    Returns:
    None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # CONFIG
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # CONNECTING TO REDSHIFT
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # RUNS THE ABOVE FUNCTIONS
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # CLOSING THE CONNECTION 
    conn.close()


if __name__ == "__main__":
    main()