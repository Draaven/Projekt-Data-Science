import os
import pandas as pd
import dask.dataframe as dd
import sqlite3
import time
#! For test
import random

# Set the working directory for fahrzeiten
#! For test
fahrzeiten_dirs = ['../../raw_data/fahrzeiten_2016/',
                   '../../raw_data/fahrzeiten_2017/',
                   '../../raw_data/fahrzeiten_2018/',
                   '../../raw_data/fahrzeiten_2019/',
                   '../../raw_data/fahrzeiten_2020/',
                   '../../raw_data/fahrzeiten_2021/',
                   '../../raw_data/fahrzeiten_2022/'
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

def get_fahrzeiten_dask_df(directory, filename, columns=None):
    if columns is None:
        columns = pd.read_csv(directory + filename, nrows=0).columns
    df = dd.read_csv(directory + filename, usecols=columns, dtype=dtypes)
    return df.compute()

# Function to get the data from the csv files and return a dataframe
def csv_to_df(filepath):
    try:
        return pd.read_csv(filepath, sep=',')
    except pd.errors.ParserError:
        return pd.read_csv(filepath, sep=';')

# Create a SQLite connection
#! For test
conn = sqlite3.connect('../../db_sqlite/data_test_small.db')

# Read the passagierfrequenz data and save it to the database
#! For test
passagierfrequenz_df = csv_to_df('../../raw_data/passagierfrequenz.csv')
passagierfrequenz_df.to_sql('passagierfrequenz', conn, if_exists='replace', index=False)

fahrzeiten_df = pd.DataFrame()

for fahrzeiten_dir in fahrzeiten_dirs:
    print(f"Processing {fahrzeiten_dir}...")
    start_time = time.time()
    year = os.path.basename(os.path.normpath(fahrzeiten_dir)).split("_")[-1]
    
    # Read the haltestelle and haltepunkt data and save them to the database
    haltestelle_df = csv_to_df(fahrzeiten_dir + 'haltestelle.csv')
    haltestelle_df.to_sql(f'haltestellen_{year}', conn, if_exists='replace', index=False)
    haltepunkt_df = csv_to_df(fahrzeiten_dir + 'haltepunkt.csv')
    haltepunkt_df.to_sql(f'haltepunkte_{year}', conn, if_exists='replace', index=False)
    
    # Read each fahrzeiten file into a DataFrame and append it to the database
    fahrzeiten_filenames = [filename for filename in os.listdir(fahrzeiten_dir) if filename.endswith('.csv') and filename.startswith('fahrzeiten_soll_ist')]
    #! For test
    fahrzeiten_filenames = random.sample(fahrzeiten_filenames, 1)
    for filename in fahrzeiten_filenames:
        df = get_fahrzeiten_dask_df(fahrzeiten_dir, filename)
        # Save the data to the database in chunks
        chunksize = int(len(df) / 10)
        df.to_sql(f'fahrzeiten_{year}', conn, if_exists='append', index=False, chunksize=chunksize)
        # Clear the DataFrame to free up memory
        fahrzeiten_df = fahrzeiten_df.iloc[0:0]
        
    print(f"Processing for {year} is complete in {time.time() - start_time} seconds")

# Close the SQLite connection
conn.close()