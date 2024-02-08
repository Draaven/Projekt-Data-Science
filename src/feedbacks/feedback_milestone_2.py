import dask.dataframe as dd
import glob
import time
import pandas as pd
from dask.distributed import Client, progress
import os
from pyproj import Proj, transform
import warnings
from IPython.display import display
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker
import multiprocessing
import sqlite3
import matplotlib.patches as mpatches
import json
import ast

if __name__ == '__main__':
    
    client = Client(n_workers=8)
    warnings.simplefilter(action='ignore', category=FutureWarning)
    
    dtypes= {
        'fahrzeiten': {
            'linie': 'Int16',
            'richtung': 'Int8',
            'fahrzeug': 'Int32',
            'kurs': 'Int16',
            'seq_von': 'Int32',
            'soll_an_von': 'Int32',
            'ist_an_von': 'Int32',
            'soll_ab_von': 'Int32',
            'ist_ab_von': 'Int32',
            'seq_nach': 'Int32',
            'soll_an_nach': 'Int32',
            'ist_an_nach': 'Int32',
            'soll_ab_nach': 'Int32',
            'ist_ab_nach': 'Int32',
            'fahrt_id': 'Int64',
            'fahrweg_id': 'Int64',
            'fw_no': 'Int16',
            'fw_typ': 'Int8',
            'fw_kurz': 'string',
            'fw_lang': 'string',
            'umlauf_von': 'Int64',
            'halt_punkt_id_von': 'Int64',
            'halt_punkt_id_nach': 'Int64'
        },
        'haltestellen': {
            'id': 'Int64',
            'diva': 'Int64',
            'halt_kurz': 'string',
            'halt_lang': 'string'
        },
        'haltepunkte': {
            'year': 'Int64',
            'id': 'Int64',
            'diva': 'Int64',
            'halt_id': 'Int64',
            'latitude': 'float64',
            'longitude': 'float64',
            'bearing': 'float64',
            'ist_aktiv': 'bool'
        },
        'passagierfrequenz': {
            'bahnhof_kurz': 'string',
            'uic': 'Int64',
            'bahnhof_lang': 'string',
            'kanton': 'string',
            'bahnhofseigner': 'string',
            'jahr': 'Int32',
            'durchschnittlicher_täglicher_verkehr': 'Int64',
            'durchschnittlicher_werktäglicher_verkehr': 'Int64',
            'durchschnittlicher_nicht_werktäglicher_verkehr': 'Int64',
            'einbezogene_bahnunternehmen': 'string',
            'bemerkungen': 'string',
            'latitude': 'float64',
            'longitude': 'float64',
            'link': 'string'
        },
        'haltestellen_liste': {
            'Name': 'string',
            'E-Koord.': 'string',
            'N-Koord.': 'string',
        },
        'fahrzeiten_old': {
            'linie': 'Int16',
            'richtung': 'Int8',
            'betriebsdatum': 'object',
            'fahrzeug': 'Int32',
            'kurs': 'Int16',
            'seq_von': 'Int32',
            'halt_diva_von': 'Int32',
            'halt_punkt_diva_von': 'Int32',
            'halt_kurz_von1': 'string',
            'datum_von': 'string',
            'soll_an_von': 'Int32',
            'ist_an_von': 'Int32',
            'soll_ab_von': 'Int32',
            'ist_ab_von': 'Int32',
            'seq_nach': 'Int32',
            'halt_diva_nach': 'Int32',
            'halt_punkt_diva_nach': 'Int32',
            'halt_kurz_nach1': 'string',
            'datum_nach': 'string',
            'soll_an_nach': 'Int32',
            'ist_an_nach1': 'Int32',
            'soll_ab_nach': 'Int32',
            'ist_ab_nach': 'Int32',
            'fahrt_id': 'Int64',
            'fahrweg_id': 'Int64',
            'fw_no': 'Int16',
            'fw_typ': 'Int8',
            'fw_kurz': 'string',
            'fw_lang': 'string',
            'umlauf_von': 'Int64',
            'halt_id_von': 'Int64',
            'halt_id_nach': 'Int64',
            'halt_punkt_id_von': 'Int64',
            'halt_punkt_id_nach': 'Int64'
        },
        'haltestellen_old': {
            'halt_id': 'Int64',
            'halt_diva': 'Int64',
            'halt_kurz': 'string',
            'halt_lang': 'string',
            'halt_ist_aktiv': 'bool'
        },
        'haltepunkte_old': {
            'halt_punkt_id': 'Int64',
            'halt_punkt_diva': 'Int64',
            'halt_id': 'Int64',
            'GPS_Latitude': 'float64',
            'GPS_Longitude': 'float64',
            'GPS_Bearing': 'float64',
            'halt_punkt_ist_aktiv': 'bool'
        },
        'passagierfrequenz_old': {
            'code_codice': 'string',
            'uic': 'Int64',
            'bahnhof_gare_stazione': 'string',
            'kt_ct_cantone': 'string',
            'isb_gi': 'string',
            'jahr_annee_anno': 'Int32',
            'dtv_tjm_tgm': 'float64',
            'dwv_tmjo_tfm': 'float64',
            'dnwv_tmjno_tmgnl': 'float64',
            'evu_ef_itf': 'string',
            'bemerkungen': 'string',
            'remarques': 'string',
            'note': 'string',
            'remarks': 'string',
            'geopos': 'string',
            'lod': 'string'
        }
    }
    
    # FEEDBACK PART 1
    # ===============
    
    # # Creating Database of Raw Data
    # # -----------------------------
    # def create_database_for_year(year):
    #     print(f'Starting with year {year}.')
    #     print('-----------------------------------')
    #     if os.path.exists(f'../../databases/raw_data_{year}.db'):
    #         os.remove(f'../../databases/raw_data_{year}.db')
                    
    #     conn = sqlite3.connect(f'../../databases/raw_data_{year}.db')
    #     c = conn.cursor()
        
    #     # Haltestellen
    #     # ------------
    #     print(f'Creating table haltestellen_{year}.')
    #     print('-----------------------------------')
    #     start_time = time.time()
        
    #     haltestellen = pd.read_csv(f'../../raw_data/fahrzeiten_{year}/haltestelle.csv', dtype=dtypes['haltestellen_old'])
    #     haltestellen_data = haltestellen.values.tolist()
        
    #     c.execute(f'''
    #         CREATE TABLE haltestellen_{year} (
    #             halt_id INTEGER PRIMARY KEY, 
    #             halt_diva INTEGER, 
    #             halt_kurz TEXT, 
    #             halt_lang TEXT, 
    #             halt_ist_aktiv INTEGER
    #         )
    #     ''')
        
    #     haltestellen_query = f'''
    #         INSERT INTO haltestellen_{year} (
    #             halt_id, halt_diva, halt_kurz, halt_lang, halt_ist_aktiv
    #         ) VALUES (?, ?, ?, ?, ?)
    #     '''
        
    #     c.executemany(haltestellen_query, haltestellen_data)
    #     conn.commit()
        
    #     current_time = time.time()
    #     time_taken = round(current_time - start_time, 2)
    #     print(f'Finished creating table haltestellen_{year} in {time_taken} seconds.')
    #     print('-----------------------------------')
        
    #     # Haltepunkte
    #     # -----------
    #     print(f'Creating table haltepunkte_{year}.')
    #     print('-----------------------------------')
    #     start_time = time.time()
        
    #     def convert_to_float(val):
    #         try:
    #             return float(val.replace(',', '.'))
    #         except:
    #             return val
        
    #     dtypes['haltepunkte_old'] = {key: value for key, value in dtypes['haltepunkte_old'].items() if key not in ['GPS_Latitude', 'GPS_Longitude', 'GPS_Bearing']}
    #     haltepunkte = pd.read_csv(f'../../raw_data/fahrzeiten_{year}/haltepunkt.csv', dtype=dtypes['haltepunkte_old'], converters={'GPS_Latitude': convert_to_float, 'GPS_Longitude': convert_to_float, 'GPS_Bearing': convert_to_float})
    #     haltepunkte_data = haltepunkte.values.tolist()
        
    #     c.execute(f'''
    #         CREATE TABLE haltepunkte_{year} (
    #             halt_punkt_id INTEGER PRIMARY KEY,
    #             halt_punkt_diva INTEGER,
    #             halt_id INTEGER,
    #             GPS_Latitude REAL DEFAULT -1.0,
    #             GPS_Longitude REAL DEFAULT -1.0,
    #             GPS_Bearing REAL DEFAULT -1.0,
    #             halt_punkt_ist_aktiv INTEGER,
    #             FOREIGN KEY (halt_id) REFERENCES haltestellen_{year}(halt_id)
    #         )
    #     ''')
        
    #     haltepunkte_query = f'''
    #         INSERT INTO haltepunkte_{year} (
    #             halt_punkt_id, halt_punkt_diva, halt_id, GPS_Latitude, GPS_Longitude, GPS_Bearing, halt_punkt_ist_aktiv
    #         ) VALUES (?, ?, ?, ?, ?, ?, ?)
    #     '''
        
    #     c.executemany(haltepunkte_query, haltepunkte_data)
    #     conn.commit()
        
    #     current_time = time.time()
    #     time_taken = round(current_time - start_time, 2)
    #     print(f'Finished creating table haltepunkte_{year} in {time_taken} seconds.')
    #     print('-----------------------------------')
        
    #     # Fahrzeiten
    #     # ----------
    #     print(f'Creating table fahrzeiten_{year}.')
    #     print('-----------------------------------')
    #     start_time = time.time()
        
    #     c.execute(f'''
    #         CREATE TABLE fahrzeiten_{year} (
    #             fahrzeiten_id INTEGER PRIMARY KEY,
    #             linie INTEGER,
    #             richtung INTEGER,
    #             betriebsdatum TEXT,
    #             fahrzeug INTEGER,
    #             kurs INTEGER,
    #             seq_von INTEGER,
    #             halt_diva_von INTEGER,
    #             halt_punkt_diva_von INTEGER,
    #             halt_kurz_von TEXT,
    #             datum_von TEXT,
    #             soll_an_von INTEGER,
    #             ist_an_von INTEGER,
    #             soll_ab_von INTEGER,
    #             ist_ab_von INTEGER,
    #             seq_nach INTEGER,
    #             halt_diva_nach INTEGER,
    #             halt_punkt_diva_nach INTEGER,
    #             halt_kurz_nach TEXT,
    #             datum_nach TEXT,
    #             soll_an_nach INTEGER,
    #             ist_an_nach INTEGER,
    #             soll_ab_nach INTEGER,
    #             ist_ab_nach INTEGER,
    #             fahrt_id INTEGER,
    #             fahrweg_id INTEGER,
    #             fw_no INTEGER,
    #             fw_typ INTEGER,
    #             fw_kurz TEXT,
    #             fw_lang TEXT,
    #             umlauf_von INTEGER,
    #             halt_id_von INTEGER,
    #             halt_id_nach INTEGER,
    #             halt_punkt_id_von INTEGER,
    #             halt_punkt_id_nach INTEGER,
    #             FOREIGN KEY (halt_punkt_id_von) REFERENCES haltepunkte_{year}(halt_punkt_id), 
    #             FOREIGN KEY (halt_punkt_id_nach) REFERENCES haltepunkte_{year}(halt_punkt_id)
    #         )
    #     ''')
        
    #     fahrzeiten_query = f'''
    #         INSERT INTO fahrzeiten_{year} (
    #             linie, richtung, betriebsdatum, fahrzeug, kurs, seq_von, 
    #             halt_diva_von, halt_punkt_diva_von, halt_kurz_von, datum_von, soll_an_von, 
    #             ist_an_von, soll_ab_von, ist_ab_von, seq_nach, halt_diva_nach, 
    #             halt_punkt_diva_nach, halt_kurz_nach, datum_nach, soll_an_nach, ist_an_nach, 
    #             soll_ab_nach, ist_ab_nach, fahrt_id, fahrweg_id, fw_no, fw_typ, fw_kurz, 
    #             fw_lang, umlauf_von, halt_id_von, halt_id_nach, halt_punkt_id_von, halt_punkt_id_nach
    #         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    #     '''
        
    #     fahrzeiten_files = glob.glob(f'../../raw_data/fahrzeiten_{year}/fahrzeiten_*.csv')
    #     for file in fahrzeiten_files:
    #         fahrzeiten = pd.read_csv(file, dtype=dtypes['fahrzeiten_old'])
    #         fahrzeiten.rename(columns={'halt_kurz_von1': 'halt_kurz_von', 'halt_kurz_nach1': 'halt_kurz_nach'}, inplace=True)
    #         fahrzeiten_data = fahrzeiten.values.tolist()
            
    #         c.executemany(fahrzeiten_query, fahrzeiten_data)
        
    #     conn.commit()
    #     current_time = time.time()
    #     time_taken = round(current_time - start_time, 2)
    #     print(f'Finished creating table fahrzeiten_{year} in {time_taken} seconds.')
    #     print('-----------------------------------')
        
    #     print(f'Creating indexes.')
    #     print('-----------------------------------')
    #     start_time = time.time()
    #     def create_index(cursor, year, table, column):
    #         index_name = f'idx_{table}_{year}_{column}'
    #         cursor.execute(f'''
    #             CREATE INDEX {index_name}
    #             ON {table}_{year}({column})
    #         ''')

    #     columns_to_index = {
    #         'haltepunkte': ['halt_punkt_id'],
    #         'haltestellen': ['halt_id'],
    #         'fahrzeiten': ['fahrzeiten_id', 'halt_diva_von', 'halt_diva_nach', 'halt_punkt_diva_von', 'halt_punkt_diva_nach', 'halt_kurz_von', 'halt_kurz_nach', 'halt_id_von', 'halt_id_nach']
    #     }

    #     for table, columns in columns_to_index.items():
    #         for column in columns:
    #             create_index(c, year, table, column)
        
    #     conn.commit()
    #     current_time = time.time()
    #     time_taken = round(current_time - start_time, 2)
    #     print(f'Finished creating indexes in {time_taken} seconds.')
    #     print('-----------------------------------')
    #     conn.close()
        
    #     print(f'Finished with year {year}.')
    #     print('-----------------------------------')
        
    # print('Starting to create databases.')
    # with multiprocessing.Pool(7) as pool:
    #     pool.map(create_database_for_year, range(2016, 2023))
    # print('Done creating databases.')
    # print('===================================')
    
    # # Performing Feedback
    # # -------------------
    # def perform_feedback_for_year(year):
    #     print(f'Starting with year {year}.')
    #     print('-----------------------------------')
    #     conn = sqlite3.connect(f'../../databases/raw_data_{year}.db')
    #     c = conn.cursor()
        
    #     counts = {
    #         'halt_punkt_diva_von': 0,
    #         'halt_punkt_diva_nach': 0,
    #         'halt_id_von': 0,
    #         'halt_id_nach': 0,
    #         'halt_diva_von': 0,
    #         'halt_diva_nach': 0,
    #         'halt_kurz_von': 0,
    #         'halt_kurz_nach': 0
    #     }
        
    #     for von_nach in ['von', 'nach']:
    #         print(f'Starting to select invalid columns for {von_nach}.')
    #         print('-----------------------------------')
    #         start_time = time.time()
            
    #         query = f'''
    #             SELECT
    #                 f.fahrzeiten_id AS key_f_id,
    #                 f.halt_punkt_id_{von_nach} AS key_hp_id,
    #                 hp.halt_id AS key_h_id,
    #                 f.halt_diva_{von_nach} AS f_halt_diva,
    #                 h.halt_diva AS h_halt_diva,
    #                 f.halt_punkt_diva_{von_nach} AS f_halt_punkt_diva,
    #                 hp.halt_punkt_diva AS hp_halt_punkt_diva,
    #                 f.halt_kurz_{von_nach} AS f_halt_kurz,
    #                 h.halt_kurz AS h_halt_kurz,
    #                 f.halt_id_{von_nach} AS f_halt_id,
    #                 h.halt_id AS h_halt_id
    #             FROM fahrzeiten_{year} f
    #             JOIN haltepunkte_{year} hp ON f.halt_punkt_id_{von_nach} = hp.halt_punkt_id
    #             JOIN haltestellen_{year} h ON hp.halt_id = h.halt_id
    #             WHERE 
    #                 f.halt_diva_{von_nach} != h.halt_diva OR
    #                 f.halt_punkt_diva_{von_nach} != hp.halt_punkt_diva OR
    #                 f.halt_kurz_{von_nach} != h.halt_kurz OR
    #                 f.halt_id_{von_nach} != h.halt_id
    #         '''
    #         df = pd.read_sql_query(query, conn)
            
    #         df.to_csv(f'../../invalid_data/invalid_columns_{von_nach}/invalid_columns_{von_nach}_{year}.csv', index=False)
            
    #         current_time = time.time()
    #         time_taken_1 = round(current_time - start_time, 2)
    #         print(f'Finished selecting invalid columns for {von_nach} in {time_taken_1} seconds.')
    #         print('-----------------------------------')
            
    #         print(f'Starting to count invalid columns for {von_nach}.')
    #         start_time = time.time()
    #         if not df.empty:
    #             for column in counts.keys():
    #                 column_no_suffix = column.replace(f'_{von_nach}', '')
    #                 if column.endswith(von_nach):
    #                     if column == (f'halt_punkt_diva_{von_nach}'):
    #                         counts[column] += len(df[df['f_halt_punkt_diva'] != df['hp_halt_punkt_diva']])
    #                     else:
    #                         counts[column] += len(df[df[f'f_{column_no_suffix}'] != df[f'h_{column_no_suffix}']])
    #         current_time = time.time()
    #         time_taken_2 = round(current_time - start_time, 2)
    #         print(f'Finished counting invalid columns for {von_nach} in {time_taken_2} seconds.')
    #         print('-----------------------------------')
        
    #     with open (f'../../invalid_data/invalid_columns_counts_{year}.txt', 'w') as file:
    #         for column, count in counts.items():
    #             file.write(f'{column}: {count}\n')
    #         file.write('-----------------------------------\n')
    #         file.write(f'Total Time: {time_taken_1 + time_taken_2} seconds')
    #         file.write('===================================')
        
    #     conn.close()
        
    # print('Starting to perform feedback.')
    # with multiprocessing.Pool(7) as pool:
    #     pool.map(perform_feedback_for_year, range(2016, 2023))
    
    # with open(f'../../invalid_data/invalid_columns_counts.txt', 'w') as outfile:
    #     for year in range(2016, 2023):
    #         with open(f'../../invalid_data/invalid_columns_counts_{year}.txt') as infile:
    #             outfile.write(f'\nYear: {year}\n')
    #             outfile.write('-----------------------------------\n')
    #             outfile.write(infile.read())
    #         os.remove(f'../../invalid_data/invalid_columns_counts_{year}.txt')
    
    # print('Done performing feedback.')
    # print('===================================')
    
    # Visualizing Feedback
    # --------------------
    '''
    Year: 2016
    -----------------------------------
    halt_punkt_diva_von: 0
    halt_punkt_diva_nach: 0
    halt_id_von: 0
    halt_id_nach: 0
    halt_diva_von: 0
    halt_diva_nach: 0
    halt_kurz_von: 0
    halt_kurz_nach: 0
    -----------------------------------
    Total Time: 76.84 seconds
    ===================================
    Year: 2017
    -----------------------------------
    halt_punkt_diva_von: 2440
    halt_punkt_diva_nach: 2598
    halt_id_von: 2728
    halt_id_nach: 2886
    halt_diva_von: 2728
    halt_diva_nach: 2886
    halt_kurz_von: 743104
    halt_kurz_nach: 743046
    -----------------------------------
    Total Time: 79.95 seconds
    ===================================
    Year: 2018
    -----------------------------------
    halt_punkt_diva_von: 0
    halt_punkt_diva_nach: 0
    halt_id_von: 0
    halt_id_nach: 0
    halt_diva_von: 0
    halt_diva_nach: 0
    halt_kurz_von: 613365
    halt_kurz_nach: 613400
    -----------------------------------
    Total Time: 85.28 seconds
    ===================================
    Year: 2019
    -----------------------------------
    halt_punkt_diva_von: 0
    halt_punkt_diva_nach: 0
    halt_id_von: 0
    halt_id_nach: 0
    halt_diva_von: 0
    halt_diva_nach: 0
    halt_kurz_von: 1024570
    halt_kurz_nach: 1024867
    -----------------------------------
    Total Time: 79.92 seconds
    ===================================
    Year: 2020
    -----------------------------------
    halt_punkt_diva_von: 0
    halt_punkt_diva_nach: 0
    halt_id_von: 0
    halt_id_nach: 0
    halt_diva_von: 0
    halt_diva_nach: 0
    halt_kurz_von: 1768
    halt_kurz_nach: 1772
    -----------------------------------
    Total Time: 75.43 seconds
    ===================================
    Year: 2021
    -----------------------------------
    halt_punkt_diva_von: 0
    halt_punkt_diva_nach: 0
    halt_id_von: 0
    halt_id_nach: 0
    halt_diva_von: 0
    halt_diva_nach: 0
    halt_kurz_von: 49674
    halt_kurz_nach: 49675
    -----------------------------------
    Total Time: 75.91 seconds
    ===================================
    Year: 2022
    -----------------------------------
    halt_punkt_diva_von: 0
    halt_punkt_diva_nach: 0
    halt_id_von: 0
    halt_id_nach: 0
    halt_diva_von: 0
    halt_diva_nach: 0
    halt_kurz_von: 185840
    halt_kurz_nach: 186232
    -----------------------------------
    Total Time: 76.8 seconds
    ===================================
    '''

    total_rows_per_year = {}
    filename = '../../invalid_data/total_rows_per_year.json'

    def process_file(file):
        df = pd.read_csv(file, dtype=dtypes['fahrzeiten_old'])
        return len(df)

    def calculate_total_rows_per_year(year):
        fahrzeiten_files = glob.glob(f'../../raw_data/fahrzeiten_{year}/fahrzeiten_*.csv')
        with multiprocessing.Pool() as pool:
            lengths = pool.map(process_file, fahrzeiten_files)
        return sum(lengths)

    filename = '../../invalid_data/total_rows_per_year.json'

    if os.path.exists(filename):
        with open(filename, 'r') as f:
            total_rows_per_year = json.load(f)
    else:
        total_rows_per_year = {}
        for year in range(2016, 2023):
            total_rows_per_year[year] = calculate_total_rows_per_year(year)

        with open(filename, 'w') as f:
            json.dump(total_rows_per_year, f)
    
    invalid_col_counts = {}
    current_year = None

    with open('../../invalid_data/invalid_columns_counts.txt', 'r') as f:
        for line in f:
            line = line.strip()
            if 'Year:' in line:
                current_year = int(line.split(': ')[1])
                invalid_col_counts[current_year] = {}
            elif ':' in line:
                key, value = line.split(': ')
                try:
                    invalid_col_counts[current_year][key.strip()] = int(value.strip())
                except ValueError:
                    continue
    
    invalid_col_percentages = {}
    for year, col_counts in invalid_col_counts.items():
        year = str(year)
        invalid_col_percentages[year] = {col: (count, count / total_rows_per_year[year] * 100) for col, count in col_counts.items()}
    
    df = pd.DataFrame(invalid_col_percentages)
    
    df_counts = df.applymap(lambda x: x[0])
    df_percentages = df.applymap(lambda x: x[1])
    
    df_counts_melted = df_counts.reset_index().melt(id_vars='index', var_name='Year', value_name='Count')
    df_percentages_melted = df_percentages.reset_index().melt(id_vars='index', var_name='Year', value_name='Percentage')
    
    df_melted = pd.merge(df_counts_melted, df_percentages_melted, on=['index', 'Year'])
    
    plt.figure(figsize=(10, 6))
    
    sns.barplot(data=df_melted, y='index', x='Percentage', hue='Year', palette='Set2')
    
    plt.title('Prozentuale Anzahl an nicht passenden Werten pro Spalte')
    plt.ylabel('')
    plt.xlabel('Prozent (%)')
    plt.grid(axis='x', zorder=1)
    plt.xlim(0, 2.1)
    plt.xticks(np.arange(0, 2.1, 0.5))

    patches = [mpatches.Patch(color=sns.color_palette('Set2')[i], label=year) for i, year in enumerate(df.columns)]
    plt.legend(handles=patches)

    plt.savefig('../../figures/cleaning/invalid_columns_counts.png', bbox_inches='tight')
    plt.close()
    
    # # FEEBACK PART 2
    # # ==============
    
    # invalid_ids_nach = pd.read_csv('../../invalid_data/invalid_ids_nach.csv', dtype=dtypes['fahrzeiten'])
    # invalid_ids_von = pd.read_csv('../../invalid_data/invalid_ids_von.csv', dtype=dtypes['fahrzeiten'])
    
    # ids_von = invalid_ids_von['halt_punkt_id_von'].unique()
    # ids_nach = invalid_ids_nach['halt_punkt_id_nach'].unique()
    
    # invalid_ids = np.union1d(ids_von, ids_nach)
    # for year in range(2018, 2023):
    #     haltepunkte = pd.read_csv(f'../../raw_data/fahrzeiten_{year}/haltepunkt.csv', dtype=dtypes['haltepunkte_old'])
    #     haltepunkte = haltepunkte['halt_punkt_id'].unique()
    #     ids_in_both = np.intersect1d(invalid_ids, haltepunkte)
    #     print(f'Number of invalid ids in {year}: {len(ids_in_both)}')
    # # -----------------------------------
    # # Output:
    # # Number of invalid ids in 2018: 0
    # # Number of invalid ids in 2019: 0
    # # Number of invalid ids in 2020: 0
    # # Number of invalid ids in 2021: 0
    # # Number of invalid ids in 2022: 0
    # # -----------------------------------