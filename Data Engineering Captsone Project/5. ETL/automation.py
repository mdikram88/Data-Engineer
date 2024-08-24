# Import libraries required for connecting to MySQL
import mysql.connector

# Import libraries required for connecting to DB2 or PostgreSQL
import psycopg2
from sqlalchemy import create_engine

# Connect to MySQL
mysql_connection = mysql.connector.connect(
    user='root',
    password='9vsFzNZon6bHsAxKRxqzRDEX',
    host='172.21.145.83',
    database='sales'
)
mysql_cursor = mysql_connection.cursor()

# Connect to DB2 or PostgreSQL
# Connection details
dsn_hostname = '172.21.14.240'
dsn_user = 'postgres'        # e.g. "abc12345"
dsn_pwd = 'Eu2Ok00tl1CmLKuMCD5Gx2h4'      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port = "5432"                # e.g. "50000" 
dsn_database = "postgres"           # i.e. "BLUDB"

postgresql_connection = psycopg2.connect(
    database=dsn_database,
    user=dsn_user,
    password=dsn_pwd,
    host=dsn_hostname,
    port=dsn_port
)
postgresql_cursor = postgresql_connection.cursor()

# Function to get the last row ID from PostgreSQL data warehouse
def get_last_rowid():
    postgresql_cursor.execute("SELECT MAX(rowid) FROM sales_data;")
    last_row = postgresql_cursor.fetchone()
    row_id = None
    if last_row and last_row[0] is not None:
        row_id = last_row[0]
    
    return row_id

last_row_id = get_last_rowid()
print("Last row id on production data warehouse = ", last_row_id)

# Function to list out all records in MySQL database with rowid greater than the one on the data warehouse
def get_latest_records(rowid):
    mysql_cursor.execute(f"SELECT * FROM sales_data WHERE rowid > {rowid};")
    return mysql_cursor.fetchall()

new_records = get_latest_records(last_row_id)
print("New rows on staging data warehouse = ", len(new_records))

# Function to insert the additional records from MySQL into PostgreSQL data warehouse
def insert_records(records):
    for row in records:
        postgresql_cursor.execute(
            "INSERT INTO sales_data(rowid,product_id,customer_id,quantity) VALUES(%s, %s, %s, %s);", 
            row
        )
    postgresql_connection.commit()  # Commit after all inserts

insert_records(new_records)
print("New rows inserted into production data warehouse = ", len(new_records))

# Disconnect from MySQL warehouse
mysql_connection.close()

# Disconnect from PostgreSQL data warehouse 
postgresql_connection.close()

# End of program
