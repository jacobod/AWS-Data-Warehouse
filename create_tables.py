import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries,copy_table_queries

print(copy_table_queries[0])

def drop_tables(cur, conn):
    '''
    Executes the drop tables queries, which remove all current tables
    for a fresh slate. Run before the create tables function.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Executes all create_table_queries, which create the staging and analytics tables in the database.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Connects to Redshift via Postgres driver, drops all tables in database, then creates the the staging and analytics tables.
    Connection is made via the parameters in the dwh.cfg file, so be sure to update before loading.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print("Connecting to Redshift..")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

    print("Connection Successful!")
    cur = conn.cursor()
    print("Dropping tables..")
    drop_tables(cur, conn)
    print("Creating tables...")
    create_tables(cur, conn)
    print("All tables created!")
    conn.close()


if __name__ == "__main__":
    main()