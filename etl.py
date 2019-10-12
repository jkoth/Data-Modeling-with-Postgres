import os                           #A portable way of using operating system dependent functionality
from logging import error, info     #Logging progress
from sys import exit                #To gracefully exit out of Python application upon error
import glob                         #Finds all the pathnames matching a specified pattern
import psycopg2                     #PostgreSQL database adapter for the Python
import pandas as pd                 #High-performance, easy-to-use data structures and data analysis tools for Python 
from sql_queries import *           #Personal library with SQL queries to be used below


"""
Purpose: 
    process_song_file function parses and processes a JSON formatted song file
    Function uses Pandas module to read song file and create DataFrame
    Artist and Song data is extracted from the DataFrame and Inserted into respective Sparkify database tables 
Arg: 
    cur - PostgreSQL connection cursor
    filepath - path where JSON formatted song file is stored
"""
def process_song_file(cur, filepath):
    # open song file
    try:
        df = pd.read_json(filepath, lines=True)
        info("Songs data files read complete")
    except Exception as e:
        error(f"Error reading Songs data files: {e}")
        exit()
    
    # insert artist record
    try:
        artist_data = df[['artist_id','artist_name','artist_location','artist_latitude', 'artist_longitude']] \
                        .values[0].tolist()
        info("Extract Artist data from Songs DataFrame successful")
    except Exception as e:
        error(f"Error creating artist table DataFrame: {e}")
        exit()
        
    try:
        cur.execute(artist_table_insert, artist_data)
        info("Successfully executed artist table INSERT statement")
    except Exception as e:
        error(f"Error executing artist table INSERT statement: {e}")
        exit()

    # insert song record
    try:
        song_data = df[['song_id','title','artist_id','year','duration']].values[0].tolist()
        info("Extract Song data from Songs DataFrame successful")
    except Exception as e:
        error(f"Error creating song table DataFrame: {e}")
        exit()

    try:
        cur.execute(song_table_insert, song_data)
        info("Successfully executed song table INSERT statement")
    except Exception as e:
        error(f"Error executing song table INSERT statement: {e}")
        exit()

    
"""
Purpose: 
    process_log_file function parses and processes a JSON formatted log file
    Function uses Pandas module to read log file and create DataFrame
    Function derives various date and time attributes using Pandas dt method on timestamp field in log file
    User and Time tables are updated using data from log file
    Songplay table is updated using data from log file along with song_id and artist_id queried from song and artist tables, respectively
Arg: 
    cur - PostgreSQL connection cursor
    filepath - path where JSON formatted log file is stored
"""
def process_log_file(cur, filepath):
    # open log file
    try:
        df = pd.read_json(filepath, lines=True)
        info("Log data files read complete")
    except Exception as e:
        error(f"Error reading log data files: {e}")
        exit()
    
    # filter by NextSong action
    try:
        df = df[(df['page']=='NextSong')]
        info("Log DataFrame filter for 'NextSong' page value successful")
    except Exception as e:
        error(f"Error filtering Log DataFrame for 'NextSong' page vale: {e}")
        exit()

    # convert timestamp column to datetime
    try:
        t = pd.to_datetime(arg=df['ts'],unit='ms')
        info("Successfully created time DataFrame by converting 'ts' attribute from log data")
    except Exception as e:
        error(f"Error time DataFrame from 'ts' attribute of log data: {e}")
        exit()
    
    # insert time data records
    # Pandas provides dtype-specific methods under various accessors 
    # 'dt' is the accessor for datetime datatype 
    # .day, .hour... are example of properties that can be accessed using 'dt' accessor on datetime field
    try:
        time_data = (df['ts'], t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
        info("Successfully created Time data DataFrame using time DataFrame and 'ts' attribute from log data")
    except Exception as e:
        error(f"Error creating Time data DataFrame: {e}")
        exit()
    
    # defining tuple of column names to create Time table DataFrame by zipping with Time data DataFrame 
    column_labels = ('timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday')

    # zipping time data tuple with column labels tuple using zip()
    # using dict() to create dictionary from zipped tuples
    # finally, creating DataFrame from dictionary
    try:
        time_df = pd.DataFrame(dict(zip(column_labels,time_data)))          
        info(f"Successfully created Time table DataFrame")
    except Exception as e:
        error(f"Error creating Time table DataFrame: {e}")
        exit()
        
    try:
        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))
        info("Successfully executed Time table INSERT statement")
    except Exception as e:
        error(f"Error executing Time table INSERT statement: {e}")
        exit()
        
    # extracting User attributes from Log data
    try:
        user_df = df[['userId','firstName','lastName','gender','level']]
        info("Successfully extracted User attributes from Log DataFrame")
    except Exception as e:
        error(f"Error extracting User attributes from Log DataFrame: {e}")
        exit()
        
    # inserting user records
    try:
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
       info("Successfully executed User table INSERT statement")
    except Exception as e:
        error(f"Error executing User table INSERT statement: {e}")
        exit()
 
    # retrieving song_id and artist_id for log data before loading to songplays table
    try:
        for index, row in df.iterrows():
            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None            
            # insert songplay record
            songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)
        info("Successfully executed Songplays table INSERT statement")
    except Exception as e:
        error(f"Error executing Songplays table INSERT statement: {e}")
        exit()


"""
Purpose: 
    process_data function builds a list of JSON files stored in the given filepath
    Itereates over list of files one at a time to process song and log data
    Calls process_song_file or process_log_file functions based on the user provided parameter
Arg: 
    cur - PostgreSQL connection cursor
    conn - PostgreSQL connection
    filepath - path where JSON formatted file is stored
    func - function (song or log) to be called to process the files in the given filepath
"""        
def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []

    try:
        for root, dirs, files in os.walk(filepath):
            files = glob.glob(os.path.join(root,'*.json'))
            for f in files :
                all_files.append(os.path.abspath(f))
        info("Creating the list of data file path complete successfully")
    except Exception as e:
        error(f"Error creating list of data file path: {e}")
        exit()

    # get total number of files found
    num_files = len(all_files)
    info(f"{num_files} files found in {filepath}")

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        info(f"{i}/{num_files} files processed")

        
"""
Purpose: 
    Connect to PostgreSQL module (psycopg2) and get the Cursor to interact with PostgreSQL database using Python
    Call process_data function with filename defaulted to paths containing song and log data
    Func is defaulted for functions to be called along with respective data
"""
def main():
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
        info("Successfully connected to Sparkify DB and retrived associated connection and cursor")
    except Exception as e:
        error(f"Error connecting to Sparkify DB: {e}")
        exit()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
