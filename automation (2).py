# Import libraries required for connecting to mysql
import mysql.connector
# Import libraries required for connecting to DB2 or PostgreSql
import psycopg2
# Connect to MySQL
connection = mysql.connector.connect(user='root', password='MTY2NjItYW5nZWxv', host='127.0.0.1', database='sales')
# cursor
cursor2 = connection.cursor()
# connection details
dsn_hostname = '127.0.0.1'
dsn_user = 'postgres'
dsn_pwd = 'Mjk5OTctYW5nZWxv'
dsn_port = "5432"
dsn_database = "sales"
# Connect to DB2 or PostgreSql
conn = psycopg2.connect(
    database=dsn_database,
    user=dsn_user,
    password=dsn_pwd,
    host=dsn_hostname,
    port=dsn_port
)
# create cursor
cursor = conn.cursor()
# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.

def get_last_rowid():
    SQL = "SELECT MAX(rowid) FROM sales_data"
    cursor.execute(SQL)
    result = cursor.fetchone()[0]
    return result

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    greater = "SELECT * FROM sales_data WHERE rowid > (%s)"
    cursor2.execute(greater, (last_row_id,))
    result = cursor2.fetchall()
    return result

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))


# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

def insert_records(records):
    insert= "INSERT INTO sales_data (rowid, product_id, customer_id, quantity) VALUES (%s, %s, %s, %s)"
    for record in records:
         cursor.execute(insert, record)
insert_records(new_records)
conn.commit()
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
connection.close()
# disconnect from DB2 or PostgreSql data warehouse 
connection.close()
# End of program