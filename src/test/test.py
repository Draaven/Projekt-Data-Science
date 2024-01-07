import mysql.connector

conn = mysql.connector.connect(user='ubuntu',
                               password='admin',
                               host='127.0.0.1',
                               database='data_test')

cursor = conn.cursor()

# Get the list of all tables
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()

for table in tables:
    table_name = table[0]
    print(table_name)

    # Get the column information
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    columns = cursor.fetchall()
    print('Length:', len(columns))
    print([column[0] for column in columns])

    # Get the number of rows in the table
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    print('{:,}'.format(cursor.fetchone()[0]))

# for table in tables:
#     table_name = table[0]
#     print(f"Dropping table {table_name}...")
    
#     # Drop the table
#     cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

conn.close()