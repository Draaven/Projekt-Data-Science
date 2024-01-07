# import os
# import pandas as pd
# import dask.dataframe as dd
# import sqlite3
# #! For test
# import random

# # Set the working directory for fahrzeiten
# #! For test
# fahrzeiten_dirs = ['../../raw_data/fahrzeiten_2016/',
#                    '../../raw_data/fahrzeiten_2017/',
#                    '../../raw_data/fahrzeiten_2018/',
#                    '../../raw_data/fahrzeiten_2019/',
#                    '../../raw_data/fahrzeiten_2020/',
#                    '../../raw_data/fahrzeiten_2021/',
#                    '../../raw_data/fahrzeiten_2022/'
#                    ]

# # Define the data types for each column
# dtypes = {
#     'linie': 'int16',
#     'richtung': 'int8',
#     'betriebsdatum': 'object',
#     'fahrzeug': 'int32',
#     'kurs': 'int16',
#     'seq_von': 'int32',
#     'halt_diva_von': 'int32',
#     'halt_punkt_diva_von': 'int32',
#     'halt_kurz_von1': 'object',
#     'datum_von': 'object',
#     'soll_an_von': 'int32',
#     'ist_an_von': 'int32',
#     'soll_ab_von': 'int32',
#     'ist_ab_von': 'int32',
#     'seq_nach': 'int32',
#     'halt_diva_nach': 'int32',
#     'halt_punkt_diva_nach': 'int32',
#     'halt_kurz_nach1': 'object',
#     'datum_nach': 'object',
#     'soll_an_nach': 'int32',
#     'ist_an_nach1': 'int32',
#     'soll_ab_nach': 'int32',
#     'ist_ab_nach': 'int32',
#     'fahrt_id': 'int64',
#     'fahrweg_id': 'int64',
#     'fw_no': 'int16',
#     'fw_typ': 'int8',
#     'fw_kurz': 'object',
#     'fw_lang': 'object',
#     'umlauf_von': 'int64',
#     'halt_id_von': 'int64',
#     'halt_id_nach': 'int64',
#     'halt_punkt_id_von': 'int64',
#     'halt_punkt_id_nach': 'int64'
# }

# def get_fahrzeiten_dask_df(directory, filename, columns=None):
#     if columns is None:
#         columns = pd.read_csv(directory + filename, nrows=0).columns
#     df = dd.read_csv(directory + filename, usecols=columns, dtype=dtypes)
#     return df.compute()

# # Function to get the data from the csv files and return a dataframe
# def csv_to_df(filepath):
#     try:
#         return pd.read_csv(filepath, sep=',')
#     except pd.errors.ParserError:
#         return pd.read_csv(filepath, sep=';')

# # Create a SQLite connection
# #! For test
# conn = sqlite3.connect('../../raw_data/data_test.db')

# # Initialize an empty DataFrame for the fahrzeiten data
# fahrzeiten_df = pd.DataFrame()

# # Loop over each directory
# for fahrzeiten_dir in fahrzeiten_dirs:
#     # Get the list of fahrzeiten filenames in the current directory
#     fahrzeiten_filenames = [filename for filename in os.listdir(fahrzeiten_dir) if filename.endswith('.csv') and filename.startswith('fahrzeiten_soll_ist')]

#     # Read each fahrzeiten file into a DataFrame and append it to fahrzeiten_df
#     for filename in fahrzeiten_filenames:
#         df = get_fahrzeiten_dask_df(fahrzeiten_dir, filename)
#         fahrzeiten_df = fahrzeiten_df.append(df, ignore_index=True)
        
#         # Save the combined fahrzeiten data to the database
#         fahrzeiten_df.to_sql(f'{os.path.basename(os.path.normpath(fahrzeiten_dir))}', conn, if_exists='append', index=False)
        
#         # Clear the DataFrame to free up memory
#         fahrzeiten_df = fahrzeiten_df.iloc[0:0]
    
#     # Read the haltestelle and haltepunkt data and save them to the database
#     haltestelle_df = csv_to_df(fahrzeiten_dir + 'haltestelle.csv')
#     haltestelle_df.to_sql(f'haltestellen_{os.path.basename(os.path.normpath(fahrzeiten_dir)).split("_")[-1]}', conn, if_exists='replace', index=False)
                          
#     haltepunkt_df = csv_to_df(fahrzeiten_dir + 'haltepunkt.csv')
#     haltepunkt_df.to_sql(f'haltepunkte_{os.path.basename(os.path.normpath(fahrzeiten_dir)).split("_")[-1]}', conn, if_exists='replace', index=False)

#     # Print a message indicating that the processing for the current year is complete
#     print(f"Processing for {os.path.basename(os.path.normpath(fahrzeiten_dir))} is complete")
# #! For test
# passagierfrequenz_df = csv_to_df('../../raw_data/passagierfrequenz.csv')

# # Save the passagierfrequenz data to the database
# passagierfrequenz_df.to_sql('passagierfrequenz', conn, if_exists='replace', index=False)

# # Close the SQLite connection
# conn.close()

# UNTIL HERE IS THE OLD CODE USING SQLITE
# -----------------------------------------------
# STARTING FROM HERE IS THE NEW CODE USING MYSQL

import os
import dask.dataframe as dd
import mysql.connector
import pandas.errors
import pandas as pd
import time
#! For test
import random
import glob

# Set the working directory for fahrzeiten
fahrzeiten_dirs = ['/data/raw_data/fahrzeiten_2016/',
                   '/data/raw_data/fahrzeiten_2017/',
                   '/data/raw_data/fahrzeiten_2018/',
                   '/data/raw_data/fahrzeiten_2019/',
                   '/data/raw_data/fahrzeiten_2020/',
                   '/data/raw_data/fahrzeiten_2021/',
                   '/data/raw_data/fahrzeiten_2022/'
                   ]

# Define the data types for each column
dtypes = {
    'linie': 'int16',
    'richtung': 'int8',
    'betriebsdatum': 'object',
    'fahrzeug': 'int32',
    'kurs': 'int16',
    'seq_von': 'int32',
    'halt_diva_von': 'int32',
    'halt_punkt_diva_von': 'int32',
    'halt_kurz_von1': 'object',
    'datum_von': 'object',
    'soll_an_von': 'int32',
    'ist_an_von': 'int32',
    'soll_ab_von': 'int32',
    'ist_ab_von': 'int32',
    'seq_nach': 'int32',
    'halt_diva_nach': 'int32',
    'halt_punkt_diva_nach': 'int32',
    'halt_kurz_nach1': 'object',
    'datum_nach': 'object',
    'soll_an_nach': 'int32',
    'ist_an_nach1': 'int32',
    'soll_ab_nach': 'int32',
    'ist_ab_nach': 'int32',
    'fahrt_id': 'int64',
    'fahrweg_id': 'int64',
    'fw_no': 'int16',
    'fw_typ': 'int8',
    'fw_kurz': 'object',
    'fw_lang': 'object',
    'umlauf_von': 'int64',
    'halt_id_von': 'int64',
    'halt_id_nach': 'int64',
    'halt_punkt_id_von': 'int64',
    'halt_punkt_id_nach': 'int64'
}

conn = mysql.connector.connect(user='ubuntu',
                               password='admin',
                               host='127.0.0.1',
                               database='data_test')

cursor = conn.cursor()

# Function to get the data from the csv files and return a dataframe
def csv_to_df(filepath):
    try:
        return dd.read_csv(filepath, sep=',', assume_missing=True)
    except pandas.errors.ParserError:
        return dd.read_csv(filepath, sep=';', assume_missing=True)

# Function to convert a pandas dtype to a MySQL dtype
def pandas_dtype_to_mysql(dtype):
    if 'object' in str(dtype):
        return 'TEXT'
    if 'float' in str(dtype):
        return 'FLOAT'
    if 'int' in str(dtype):
        return 'INT'
    return 'TEXT'

# Function to create a table in the database and insert the data
def create_table(df, table_name, conn):
    cursor = conn.cursor()
    
    # Print message before table creation
    print(f"Starting creation of table {table_name}...")
    
    # Record the start time
    start_time = time.time()
    
    # Create table
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    create_query = f"CREATE TABLE {table_name} ({', '.join([f'{col} {pandas_dtype_to_mysql(dtype)}' for col, dtype in zip(df.columns, df.dtypes)])})"
    cursor.execute(create_query)
    
    # Function to convert a Dask DataFrame row to a list, replacing 'nan' with None
    def row_to_list(row):
        return [None if pd.isna(val) else val for val in row]
    
    # Insert data
    df_columns = list(df)
    columns = ",".join(df_columns)
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 
    insert_stmt = "INSERT INTO {} ({}) {}".format(table_name,columns,values)
    data_to_insert = df.map_partitions(lambda df: df.apply(row_to_list, axis=1), meta=('object')).compute(scheduler='threads').tolist()
    cursor.executemany(insert_stmt, data_to_insert)
    conn.commit()
    
    # Calculate and print the time taken
    end_time = time.time()
    time_taken = end_time - start_time
    print(f"Time taken to create table {table_name}: {time_taken} seconds.")

# Load and insert the passagierfrequenz data
passagierfrequenz_df = csv_to_df('../../raw_data/passagierfrequenz.csv')
create_table(passagierfrequenz_df, 'passagierfrequenz', conn)

# Load and insert the fahrzeiten, haltestellen, and haltepunkte data for each year
for fahrzeiten_dir in fahrzeiten_dirs:
    year = os.path.basename(os.path.normpath(fahrzeiten_dir)).split("_")[-1]
    
    # Load and insert the haltestellen data
    haltestellen_df = csv_to_df(fahrzeiten_dir + 'haltestelle.csv')
    create_table(haltestellen_df, f'haltestellen_{year}', conn)
    
    # Load and insert the haltepunkte data
    haltepunkte_df = csv_to_df(fahrzeiten_dir + 'haltepunkt.csv')
    create_table(haltepunkte_df, f'haltepunkte_{year}', conn)

    # Load and insert the fahrzeiten data
    # fahrzeiten_df = dd.read_csv(fahrzeiten_dir + 'fahrzeiten_*.csv', dtype=dtypes)
    #! For test
    all_files = glob.glob(fahrzeiten_dir + 'fahrzeiten_*.csv')
    selected_files = random.sample(all_files, 10)
    fahrzeiten_df = dd.read_csv(selected_files, dtype=dtypes)
    create_table(fahrzeiten_df, f'fahrzeiten_{year}', conn)
    
# Close the MySQL connection
conn.close()