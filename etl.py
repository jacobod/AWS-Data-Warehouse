import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Executes the copy table queries, in order to create the staging tables.
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Executes the insert_table_queries, which move data from the staging tables
    to their respective analytics table.
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Connects to Redshift via Postgres, copies the the staging tables from designated json files via config file, 
    then moves the data from the staging tables to analytics tables. 
    Connection is made via the parameters in the dwh.cfg file, so be sure to update before loading.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Loading Staging tables..")
    load_staging_tables(cur, conn)
    print("Now loading staging data to analytics tables..")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()