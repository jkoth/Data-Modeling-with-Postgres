import psycopg2                                                     #Python Postgres module
from sql_queries import create_table_queries, drop_table_queries    #Importing variables containing SQL defined in sql_queries
from logging import error, info                                     #Logging progress
from sys import exit                                                #To gracefully exit Python application upon error

"""
Purpose: 
    Connects to the default Postgres database using psycopg2 module
    Drops the Sparkify database, if exists and Creates a new Sparkify database
    Connects to the Sparkify database and retrieves Cursor
Return: 
    Function returns connection and cursor to the Sparkify database    
"""
def create_database():
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn

"""
Purpose: 
    drop_tables function imports SQL DROP Table statements stored as a list in drop_table_queries variable in sql_queries file
    and executes each statement one at a time using FOR loop
Arg: 
    conn - Connection to Sparkify database
    cur - Connection Cursor to the Sparkify database
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

"""
Purpose: 
    create_tables imports SQL CREATE Table statements stored as a list in create_table_queries variable in sql_queries file
    and executes each statement one at a time using FOR loop
Arg: 
    conn - Connection to Sparkify database
    cur - Connection Cursor to the Sparkify database
"""

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

"""
Purpose: 
    Calls create_database function to drop and create Sparkify database
    Captures database Connection and Cursor in variables
    Calls drop_tables and create_tables functions with database Connection and Cursor as function arguments
"""
def main():

    try:
        cur, conn = create_database()
        info("Sucessfully created database and retrieved associated connection and cursor")
    except Exception as e:
        error(f"Error creating database or retrieving associated connection and cursor: {e}")
        exit()
    
    try:
        drop_tables(cur, conn)
        info("DROP TABLE queries execution complete")
    except Exception as e:
        error(f"Error executing DROP TABLE queries: {e}")
        exit()
    
    try:
        create_tables(cur, conn)
        info("CREATE TABLE queries execution complete")
    except Exception as e:
        error(f"Error executing CREATE TABLE queries: {e}")
        exit()

    conn.close()

if __name__ == "__main__":
    main()
