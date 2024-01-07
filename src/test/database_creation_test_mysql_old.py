# import os
# import dask.dataframe as dd
# import mysql.connector
# import pandas.errors
# import pandas as pd
# import time
# from sqlalchemy import create_engine
# #! For test
# import random
# import glob

# # Set the working directory for fahrzeiten
# fahrzeiten_dirs = ['/data/raw_data/fahrzeiten_2016/',
#                    '/data/raw_data/fahrzeiten_2017/',
#                    '/data/raw_data/fahrzeiten_2018/',
#                    '/data/raw_data/fahrzeiten_2019/',
#                    '/data/raw_data/fahrzeiten_2020/',
#                    '/data/raw_data/fahrzeiten_2021/',
#                    '/data/raw_data/fahrzeiten_2022/'
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

# conn = mysql.connector.connect(user='ubuntu',
#                                password='StrongPassword123!',
#                                host='127.0.0.1',
#                                database='data_test')

# cursor = conn.cursor()
# engine_string = 'mysql+mysqlconnector://ubuntu:StrongPassword123!@localhost/data_test'
# engine = create_engine(engine_string)

# # Function to get the data from the csv files and return a dataframe
# def csv_to_df(filepath):
#     try:
#         return dd.read_csv(filepath, sep=',', assume_missing=True)
#     except pandas.errors.ParserError:
#         return dd.read_csv(filepath, sep=';', assume_missing=True)

# # Function to convert a pandas dtype to a MySQL dtype
# def pandas_dtype_to_mysql(dtype):
#     if 'object' in str(dtype):
#         return 'TEXT'
#     if 'float' in str(dtype):
#         return 'FLOAT'
#     if 'int' in str(dtype):
#         return 'INT'
#     return 'TEXT'

# # Function to create a table in the database and insert the data
# def create_table(df, table_name, append=False):
#     print(f"Starting creation of table {table_name}...")
#     start_time = time.time()
    
#     # Write the DataFrame to the SQL database
#     #! For Test if_exists='fail' remember to change to 'replace'
#     try:
#         if append:
#             df.to_sql(table_name, engine_string, if_exists='append', index=False)
#         else:
#             df.to_sql(table_name, engine_string, if_exists='fail', index=False)
#     except ValueError:
#         print(f"Table {table_name} already exists.")
#         return
    
#     end_time = time.time()
#     time_taken = end_time - start_time
#     print(f"Time taken to create table {table_name}: {time_taken} seconds.")

# # Load and insert the passagierfrequenz data
# passagierfrequenz_df = csv_to_df('../../raw_data/passagierfrequenz.csv')
# create_table(passagierfrequenz_df, 'passagierfrequenz')

# # Load and insert the fahrzeiten, haltestellen, and haltepunkte data for each year
# for fahrzeiten_dir in fahrzeiten_dirs:
#     year = os.path.basename(os.path.normpath(fahrzeiten_dir)).split("_")[-1]
    
#     # Load and insert the haltestellen data
#     haltestellen_df = csv_to_df(fahrzeiten_dir + 'haltestelle.csv')
#     create_table(haltestellen_df, f'haltestellen_{year}')
    
#     # Load and insert the haltepunkte data
#     haltepunkte_df = csv_to_df(fahrzeiten_dir + 'haltepunkt.csv')
#     create_table(haltepunkte_df, f'haltepunkte_{year}')

#     # Load and insert the fahrzeiten data
#     # fahrzeiten_df = dd.read_csv(fahrzeiten_dir + 'fahrzeiten_*.csv', dtype=dtypes)
#     #! For test
#     files_processed = 0
#     for filename in glob.glob(fahrzeiten_dir + 'fahrzeiten_*.csv'):
#         if files_processed >= 10:
#             break
#         fahrzeiten_df = dd.read_csv(filename, dtype=dtypes)
#         create_table(fahrzeiten_df, f'fahrzeiten_{year}', append=True)
    
# # Close the MySQL connection
# conn.close()