import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop tables in the database according to specified SQL queries.

    This function iterates through a list of SQL queries specified in the 'drop_table_queries' list
    and executes each query to drop tables in the database. Each query in the list should be responsible
    for dropping a specific table. All tables should be dropped if exist. 

    Parameters:
    cur (psycopg2.cursor): The database cursor used to execute SQL queries.
    conn (psycopg2.connect): The database connection object.

    Returns:
    None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create tables in the database according to specified SQL queries.

    This function iterates through a list of SQL queries specified in the 'create_table_queries' list
    and executes each query to create tables in the database. The table created include:
     1. Staging tables: staging_event, staging_song. 
     2. Fact table: songplay
     3. Dimension tables: song, user, time, artist. 

    Parameters:
    cur (psycopg2.cursor): The database cursor used to execute SQL queries.
    conn (psycopg2.connect): The database connection object.

    Returns:
    None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()