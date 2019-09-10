ta Modeling with Postgres**
## **Project Summary**
The project goal was to build a Postgres schema with the tables designed to optimize queries on song play analysis. Songs and User activity data are collected in the startup company, Sparkify’s music streaming app. Data is stored in JSON format and not easily available for analysis. As part of the solution, a five table Star schema was defined and built an ETL pipeline to transfer data from Songs and User activity files stored in the local directories to the tables. Utilized multiple Python modules to extract the data on local directories, transform them and load them to Postgres.

## **Python Scripts**
Python script is broken down into three files.  <br>
- “sql_queries.py” contains SQL code for Create table definitions, Drop table statement, Insert statement, and Select query for Songs Play table. SQL is assigned to multiple variables which are referenced in other files.
- “create_tables.py” contains Python functions that connects to Postgres database using “psycopg2” Python Postgres module and drops database, creates database, drops tables, and creates tables. SQL code is imported from “sql_queries.py” file. 
- “etl.py” contains Python functions that processes data from JSON formatted files stored on local drives and insert them into respective tables. Python modules “os” and “glob” are used to interact with the operating system where the data files are stored. Pandas module is used to efficiently read and parse JSON files. Finally, INSERT statements are imported from “sql_queries.py” file.

These files must be updated and processed sequentially. SQL file must be updated with all the SQL code to be used in other two files. Create tables python script must be run before ETL script to make sure required database and tables are created before inserting data.

## **Project Datasets**
### **Songs Dataset**
Subset of the original dataset, Million Song Dataset (Columbia University). Contains JSON formatted files with metadata about a song and its artist. One file contains metadata for one song. Files are partitioned by the first three letters of each song's track ID

### **Log Dataset**
This dataset is generated using Eventsim simulator hosted on Github. Each file is JSON formatted and contains user activity for a given day. Files are partitioned by year and month.

