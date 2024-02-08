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
    
    # DELETING TIME VALUE OF COLUMN IN CLEANING PROCESSES DONE
    # ========================================================
    # cleaning_processes_done = pd.read_csv('../cleaning_processes_done.csv')
    # # Deleting Time value of Haltestellen and Haltepunkte
    # cleaning_processes_done.loc[(cleaning_processes_done['Process'].str.contains('Haltestellen')) | (cleaning_processes_done['Process'].str.contains('Haltepunkte')), 'Time'] = ''
    # # Deleting all Time and Output values
    # cleaning_processes_done['Time'] = ''
    # cleaning_processes_done['Output'] = ''
    # cleaning_processes_done.to_csv('../cleaning_processes_done.csv', index=False)
            
    # CREATING TEST DATA
    # ==================
    # for year in range(2016, 2023):
    #     df = dd.read_csv(f'../../cleaned_data/fahrzeiten_{year}.csv/*', assume_missing=True, dtype=dtypes['fahrzeiten'])
    #     df = df.sample(frac=0.1)
    #     df.to_csv(f'../../test_data/fahrzeiten_{year}.csv', single_file=False, index=False)
    
    # CREATING TEST DATA SINGLE FILE
    # ==============================    
    # for year in range(2016, 2023):
    #     df = dd.read_csv(f'../../test_data/fahrzeiten_{year}.csv/*', assume_missing=True, dtype=dtypes['fahrzeiten'])
    #     df.to_csv(f'../../test_data/fahrzeiten_{year}_single.csv', single_file=True, index=False)
    # df = dd.read_csv(f'../../test_data/fahrzeiten_2016.csv/*', assume_missing=True, dtype=dtypes['fahrzeiten'])
    # df.to_csv(f'../../test_data/fahrzeiten_2016_single_2.csv', single_file=True, index=False)
    
    # PUTTING INVALID IDS IN ONE FILE
    # ===============================
    # dfs_von = []
    # dfs_nach = []
    # for year in range(2016, 2023):
    #     # Von
    #     df_von = dd.read_csv(f'../../invalid_data/invalid_ids_von_{year}.csv/*', assume_missing=True, dtype=dtypes['fahrzeiten'])
    #     df_von['year'] = year
    #     df_von = df_von.map_partitions(lambda df: df[['year'] + [col for col in df.columns if col != 'year']])
    #     dfs_von.append(df_von)
    #     # Nach
    #     df_nach = dd.read_csv(f'../../invalid_data/invalid_ids_nach_{year}.csv/*', assume_missing=True, dtype=dtypes['fahrzeiten'])
    #     df_nach['year'] = year
    #     df_nach = df_nach.map_partitions(lambda df: df[['year'] + [col for col in df.columns if col != 'year']])
    #     dfs_nach.append(df_nach)
    # df_concat_von = dd.concat(dfs_von)
    # df_concat_nach = dd.concat(dfs_nach)
    # df_concat_von.to_csv('../../invalid_data/invalid_ids_von.csv', single_file=True, index=False)
    # df_concat_nach.to_csv('../../invalid_data/invalid_ids_nach.csv', single_file=True, index=False)
    
    # PRINT OUT THE ROWS, WHERE THE INVALID ID OCCURS
    # ===============================================
    # dtypes_fahrzeiten = {
    #     'betriebsdatum': 'string',
    #     'soll_an_von': 'Int32',
    #     'ist_an_von': 'Int32',
    #     'soll_ab_von': 'Int32',
    #     'ist_ab_von': 'Int32',
    #     'halt_id_von': 'Int64',
    #     'halt_id_nach': 'Int64',
    #     'halt_punkt_id_von': 'Int64',
    #     'halt_punkt_id_nach': 'Int64',
    # }
    # fahrzeiten_2017_old = dd.read_csv('../../raw_data/fahrzeiten_2017/fahrzeiten_*.csv', assume_missing=True, dtype=dtypes_fahrzeiten, usecols=dtypes_fahrzeiten.keys())
    # invalid_ids_nach = dd.read_csv('../../invalid_data/invalid_ids_nach.csv', assume_missing=True)
    # invalid_ids_von = dd.read_csv('../../invalid_data/invalid_ids_von.csv', assume_missing=True)
    # selected_rows = fahrzeiten_2017_old[fahrzeiten_2017_old['halt_punkt_id_nach'] == 14271]
    # print(selected_rows.compute())
        
    
    # TESTING
    # =======
    def process_invalid_columns(year, von_nach='von'):
        if von_nach == 'von':
            invalid_df = pd.read_csv(f'../../invalid_data/invalid_columns_von/invalid_columns_von_{year}.csv')
        else:
            invalid_df = pd.read_csv(f'../../invalid_data/invalid_columns_nach/invalid_columns_nach_{year}.csv')

        with open (f'../../invalid_data/invalid_columns_values_{year}.txt', 'a') as file:
            if invalid_df.empty and von_nach == 'von':
                file.write(f'Year: {year}\n')
                process_invalid_columns(year, 'nach')
                return
            elif invalid_df.empty and von_nach == 'nach':
                file.write('===================================\n')
                return

            if von_nach == 'von':
                file.write(f'Year: {year}\n')
                file.write('-----------------------------------\n')
            file.write(f'Invalid Columns {von_nach.capitalize()}:\n')
            file.write('Column, Value in Fahrzeiten, Value in Haltestellen, Count\n')

            columns = {}
            for col in invalid_df.columns:
                if col.startswith('f_'):
                    corresponding_col = None
                    if 'h_' + col[2:] in invalid_df.columns:
                        corresponding_col = 'h_' + col[2:]
                    elif 'hp_' + col[2:] in invalid_df.columns:
                        corresponding_col = 'hp_' + col[2:]
                    if corresponding_col:
                        columns[col] = corresponding_col

            for column, corresponding_column in columns.items():
                unique_pairs = invalid_df[invalid_df[column] != invalid_df[corresponding_column]][[column, corresponding_column]].drop_duplicates().values
                for pair in unique_pairs:
                    filtered_df = invalid_df[(invalid_df[column] == pair[0]) & (invalid_df[corresponding_column] == pair[1])]
                    if filtered_df.empty:
                        continue
                    fahrzeiten_value = pair[0]
                    other_value = pair[1]

                    count = len(filtered_df)

                    file.write(f'{column}, {fahrzeiten_value}, {other_value}, {count}\n')

            if von_nach == 'von':
                file.write('-----------------------------------\n')
            else:
                file.write('===================================\n')
        
        if von_nach == 'von':
            process_invalid_columns(year, 'nach')
    
    for year in range(2016, 2023):
        process_invalid_columns(year)
        
    with open(f'../../invalid_data/invalid_columns_values.txt', 'w') as outfile:
        for year in range(2016, 2023):
            with open(f'../../invalid_data/invalid_columns_values_{year}.txt') as infile:
                outfile.write(infile.read())
            os.remove(f'../../invalid_data/invalid_columns_values_{year}.txt')