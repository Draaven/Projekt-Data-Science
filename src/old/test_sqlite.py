import sqlite3

# Small database
conn = sqlite3.connect('../../db_sqlite/data_test_small.db')
# Test database
# conn = sqlite3.connect('../../db_sqlite/data_test.db')
# Full database
# conn = sqlite3.connect('../../db_sqlite/data.db')

cursor = conn.cursor()

# Get the list of all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

# PRINTING TABLES
# for table in tables:
#     table_name = table[0]
#     print(table_name)

#     # Get the column information
#     cursor.execute(f"PRAGMA table_info({table_name})")
#     columns = cursor.fetchall()
#     print('Length:', len(columns))
#     print([column[1] for column in columns])  # column[1] is the column name

#     # Get the number of rows in the table
#     cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
#     print('{:,}'.format(cursor.fetchone()[0]))

for table in tables:
    table_name = table[0]
    
    # DELETING SPECIFIC TABLE
    # if table_name == 'fahrzeiten_2016':
    #     print(f"Dropping table {table_name}...")
        
    #     # Drop the table
    #     cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    # DELETING ALL TABLES
    # print(f"Dropping table {table_name}...")
    
    # # Drop the table
    # cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

conn.close()