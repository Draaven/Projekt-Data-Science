import os
import pandas as pd
import numpy as np
import dask.dataframe as dd
import time
import glob
from dask.distributed import Client, progress
import re
import warnings

if __name__ == '__main__':
    
    client = Client(n_workers=8)
    warnings.simplefilter(action='ignore', category=FutureWarning)

    # Set the working directory for fahrzeiten
    fahrzeiten_dirs = ['../raw_data/fahrzeiten_2016/',
                    '../raw_data/fahrzeiten_2017/',
                    '../raw_data/fahrzeiten_2018/',
                    '../raw_data/fahrzeiten_2019/',
                    '../raw_data/fahrzeiten_2020/',
                    '../raw_data/fahrzeiten_2021/',
                    '../raw_data/fahrzeiten_2022/'
                    ]

    dtypes = {
        'fahrzeiten': {
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
        'haltestellen': {
            'halt_id': 'Int64',
            'halt_diva': 'Int64',
            'halt_kurz': 'string',
            'halt_lang': 'string',
            'halt_ist_aktiv': 'bool'
        },
        'haltepunkte': {
            'halt_punkt_id': 'Int64',
            'halt_punkt_diva': 'Int64',
            'halt_id': 'Int64',
            'GPS_Latitude': 'string',
            'GPS_Longitude': 'string',
            'GPS_Bearing': 'float64',
            'halt_punkt_ist_aktiv': 'bool'
        },
        'passagierfrequenz': {
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

    rename_deletion_operations = {
        'fahrzeiten': [
            'RENAME ist_an_nach1 TO ist_an_nach',
            'DROP halt_diva_von',
            'DROP halt_punkt_diva_von',
            'DROP halt_kurz_von1',
            'DROP halt_diva_nach',
            'DROP halt_punkt_diva_nach',
            'DROP halt_kurz_nach1',
            'DROP halt_id_von',
            'DROP halt_id_nach',
        ],
        'haltestellen': [
            'RENAME halt_id TO id',
            'RENAME halt_diva TO diva',
            'DROP halt_ist_aktiv',
        ],
        'haltepunkte': [
            'RENAME halt_punkt_id TO id',
            'RENAME halt_punkt_diva TO diva',
            'RENAME GPS_Latitude TO latitude',
            'RENAME GPS_Longitude TO longitude',
            'RENAME GPS_Bearing TO bearing',
            'RENAME halt_punkt_ist_aktiv TO ist_aktiv',
        ],
        'passagierfrequenz': [
            'RENAME code_codice TO bahnhof_kurz',
            'RENAME bahnhof_gare_stazione TO bahnhof_lang',
            'RENAME kt_ct_cantone TO kanton',
            'RENAME isb_gi TO bahnhofseigner',
            'RENAME jahr_annee_anno TO jahr',
            'RENAME dtv_tjm_tgm TO durchschnittlicher_täglicher_verkehr',
            'RENAME dwv_tmjo_tfm TO durchschnittlicher_werktäglicher_verkehr',
            'RENAME dnwv_tmjno_tmgnl TO durchschnittlicher_nicht_werktäglicher_verkehr',
            'RENAME evu_ef_itf TO einbezogene_bahnunternehmen',
        ]
    }
    
    unify_operations = {
        'haltestellen': [
            'REPLACE AT halt_lang , WITH ;',
            'REPLACE AT halt_lang " WITH '
        ],
        'haltepunkte': [
            'REPLACE AT latitude " WITH ',
            'REPLACE AT longitude " WITH ',
            'REPLACE AT latitude , WITH .',
            'REPLACE AT longitude , WITH .',
            'CHECK . IN bearing',
        ],
        'passagierfrequenz': [
            'REPLACE AT einbezogene_bahnunternehmen , WITH ;',
            'REPLACE AT einbezogene_bahnunternehmen " WITH ',
            'CHECK . IN durchschnittlicher_täglicher_verkehr',
            'CHECK . IN durchschnittlicher_werktäglicher_verkehr',
            'CHECK . IN durchschnittlicher_nicht_werktäglicher_verkehr',
        ]
    }

    processes_done = pd.read_csv('cleaning_processes_done.csv', index_col=False)

    # All: Function to rename and delete columns
    def rename_and_delete(df, table_name):
        table_name = table_name.split('_')[0]
        for operation in rename_deletion_operations[table_name]:
            if 'RENAME' in operation:
                df = df.rename(columns={operation.split(' ')[1]: operation.split(' ')[-1]})
            elif 'DROP' in operation:
                df = df.drop(columns=operation.split(' ')[1])
        output = 'True'
        return df, output
    
    # All: Function to delete duplicate rows
    def delete_duplicates(df, table_name):
        initial_row_count = len(df)
        df = df.repartition(npartitions=df.npartitions)
        df = df.map_partitions(lambda part: part.drop_duplicates())
        final_row_count = len(df)
        duplicate_rows = initial_row_count - final_row_count
        output = f'Duplicate Rows: {duplicate_rows}'
        return df, output

    # All: Function to fill empty cells uniformly
    def unify_empty(df, table_name):
        df = df.replace({'': np.nan, 'None': np.nan, 'Null': np.nan, None: np.nan})
        empty_count = df.isnull().sum().sum().compute()
        output = f'Empty Cells: {empty_count}'
        return df, output
    
    # All: Function to unify values
    def unify_values(df, table_name):
        table_name = table_name.split('_')[0]
        for operation in unify_operations[table_name]:
            operation_parts = operation.split(' ')
            if 'REPLACE' in operation:
                column_name = operation_parts[2]
                old_value = operation_parts[3]
                new_value = operation_parts[5]
                df[column_name] = df[column_name].str.replace(old_value, new_value)
            elif 'CHECK' in operation:
                column_name = operation_parts[3]
                if df[column_name].astype(str).str.contains('\.0+$').all().compute():
                    df[column_name] = df[column_name].astype(str).str.replace('\.0+$', '').astype(float).astype(int)
        output = 'True'
        return df, output
    
    # Fahrzeiten: Function to use datetime format for date columns
    def change_date_format(df, table_name):
        date_cols = ['betriebsdatum', 'datum_von', 'datum_nach']
        initial_valid_dates = sum(df[col].notnull().sum().compute() for col in date_cols)
        for col in date_cols:
            df[col] = df[col].map_partitions(pd.to_datetime, format='%d.%m.%y', errors='coerce')
        final_valid_dates = sum(df[col].notnull().sum().compute() for col in date_cols)
        false_dates = initial_valid_dates - final_valid_dates
        output = f'False Dates: {false_dates}'
        return df, output
    
    # Fahrzeiten: Function to check if dates are all the same
    def check_dates(df, table_name):
        date_cols = ['betriebsdatum', 'datum_von', 'datum_nach']
        not_na = df[date_cols].notnull().all(axis=1)
        inconsistent_rows = ((df.loc[not_na, 'betriebsdatum'] != df.loc[not_na, 'datum_von']) | 
                             (df.loc[not_na, 'betriebsdatum'] != df.loc[not_na, 'datum_nach']) | 
                             (df.loc[not_na, 'datum_von'] != df.loc[not_na, 'datum_nach'])).sum()
        false_dates = df[date_cols].isnull().any(axis=1).sum()
        inconsistent_rows, false_dates = dd.compute(inconsistent_rows, false_dates)
        if inconsistent_rows == 0:
            df = df.drop(columns=date_cols[1:])
        output = f'Different Dates in Rows: {inconsistent_rows}; False Dates: {false_dates}'
        return df, output
        
    # Passagierfrequenz: Function to split geopos column into latitude and longitude
    def split_geopos(df, table_name):
        df[['latitude', 'longitude']] = df['geopos'].str.split(', ', n=1, expand=True)
        df['link'] = df['lod']
        df = df.drop(columns=['geopos', 'lod'])
        output = 'True'
        return df, output

    # Passagierfrequenz: Function to check if remarks are consistent
    def check_remarks(df, table_name):
        inconsistent_rows = 0
        remark_cols = ['bemerkungen', 'remarques', 'note', 'remarks']
        for _, row in df.iterrows():
            empty_counts = 0
            not_empty_counts = 0
            for col in remark_cols:
                if pd.isna(row[col]) or row[col] == '':
                    empty_counts += 1
                else:
                    not_empty_counts += 1
            if empty_counts > 0 and not_empty_counts > 0:
                inconsistent_rows += 1
        if inconsistent_rows == 0:
            df = df.drop(columns=remark_cols[1:])
        output = f'Inconsistent Rows: {inconsistent_rows}'
        return df, output

    # Function to process the data with the above functions
    def process_data(functions, df, table_name):
        done_something = False
        for function, process_name in functions:
            process = processes_done.loc[processes_done['Process'] == process_name]
            if pd.isnull(process['Time'].item()):
                done_something = True
                print(f"Working on {process_name.replace('_', ' ')}...")
                start_time = time.time()
                df, output = function(df, table_name)
                current_time = time.time()
                time_taken = round(current_time - start_time, 2)
                processes_done.loc[processes_done['Process'] == process_name, 'Time'] = time_taken
                processes_done.loc[processes_done['Process'] == process_name, 'Output'] = output
                print(f"Finished {process_name.replace('_', ' ')} in {time_taken} seconds.")
        if done_something:
            start_time = time.time()
            print(f'Writing {table_name} to csv...')
            if table_name.startswith('fahrzeiten'):
                df.to_csv(f'../cleaned_data/{table_name}.csv', index=False, single_file=False)
            else:
                df.to_csv(f'../cleaned_data/{table_name}.csv', index=False, single_file=True)
            current_time = time.time()
            time_taken = round(current_time - start_time, 2)
            print(f'Finished writing {table_name} to csv in {time_taken} seconds.')
            processes_done.to_csv('cleaning_processes_done.csv', index=False)
    
    # Passagierfrequenz
    # ========================================
    passagierfrequenz_df = dd.read_csv('../raw_data/passagierfrequenz.csv', sep=';', dtype=dtypes['passagierfrequenz'])

    functions = [
        (rename_and_delete, 'Passagierfrequenz_Renaming_And_Deletion'),
        (delete_duplicates, 'Passagierfrequenz_Deleting_Duplicate_Rows'),
        (unify_empty, 'Passagierfrequenz_Unifying_Empty'),
        (unify_values, 'Passagierfrequenz_Unifying_Values'),
        (split_geopos, 'Passagierfrequenz_Splitting_Geopos'),
        (check_remarks, 'Passagierfrequenz_Checking_Remarks'),
    ]

    process_data(functions, passagierfrequenz_df, 'passagierfrequenz')
    
    print('Finished Passagierfrequenz.')
    print('========================================')

    # Fahrzeiten, Haltestellen, Haltepunkte
    # ========================================
    for fahrzeiten_dir in fahrzeiten_dirs:
        year = fahrzeiten_dir.split('_')[-1].replace('/', '')
        print(f'Working on Fahrzeiten for {year}...')
        print('----------------------------------------')
        
        # Haltestellen
        # ----------------------------------------
        haltestellen_df = dd.read_csv(fahrzeiten_dir + 'haltestelle.csv', dtype=dtypes['haltestellen'])
        
        functions = [
            (rename_and_delete, f'Haltestellen_{year}_Renaming_And_Deletion'),
            (delete_duplicates, f'Haltestellen_{year}_Deleting_Duplicate_Rows'),
            (unify_empty, f'Haltestellen_{year}_Unifying_Empty'),
            (unify_values, f'Haltestellen_{year}_Unifying_Values'),
        ]
        
        process_data(functions, haltestellen_df, f'haltestellen_{year}')
        
        print(f'Finished Haltestellen for {year}.')
        print('----------------------------------------')
        
        # Haltepunkte
        # ----------------------------------------
        haltepunkte_df = dd.read_csv(fahrzeiten_dir + 'haltepunkt.csv', dtype=dtypes['haltepunkte'], decimal=',')
        
        functions = [
            (rename_and_delete, f'Haltepunkte_{year}_Renaming_And_Deletion'),
            (delete_duplicates, f'Haltepunkte_{year}_Deleting_Duplicate_Rows'),
            (unify_empty, f'Haltepunkte_{year}_Unifying_Empty'),
            (unify_values, f'Haltepunkte_{year}_Unifying_Values'),
        ]
        
        process_data(functions, haltepunkte_df, f'haltepunkte_{year}')
        
        print(f'Finished Haltepunkte for {year}.')
        print('----------------------------------------')
        
        # Fahrzeiten
        # ----------------------------------------
        fahrzeiten_files = glob.glob(fahrzeiten_dir + 'fahrzeiten*.csv')
        fahrzeiten_df = dd.concat([dd.read_csv(f, sep=',', dtype=dtypes['fahrzeiten']) for f in fahrzeiten_files], ignore_index=True)
        
        functions = [
            (rename_and_delete, f'Fahrzeiten_{year}_Renaming_And_Deletion'),
            (delete_duplicates, f'Fahrzeiten_{year}_Deleting_Duplicate_Rows'),
            (unify_empty, f'Fahrzeiten_{year}_Unifying_Empty'),
            (change_date_format, f'Fahrzeiten_{year}_Changing_Date_Format'),
            (check_dates, f'Fahrzeiten_{year}_Checking_Dates'),
        ]
        
        process_data(functions, fahrzeiten_df, f'fahrzeiten_{year}')
        
        print(f'Finished Fahrzeiten for {year}.')
        print('========================================')

    # Sum of Time Taken for Each Process
    # ========================================
    different_processes_with_years = processes_done.loc[processes_done['Process'].str.contains('_20')]
    process_groups = different_processes_with_years['Process'].str.split('_20').str[1].str[3:].unique()
    tables = ['Fahrzeiten', 'Haltestellen', 'Haltepunkte']

    for table in tables:
        for group in process_groups:
            process_time_sum = 0
            process_output_sum = {}
            processes = different_processes_with_years.loc[
                (different_processes_with_years['Process'].str.contains(table)) &
                (different_processes_with_years['Process'].str.contains(group))
            ].copy()
            processes['Time'] = processes['Time'].fillna(0)
            for index, row in processes.iterrows():
                process_time_sum += row['Time']
                if row['Output'] != 'True':
                    outputs = row['Output'].split('; ')
                    for output in outputs:
                        key, value = output.split(': ')
                        if key not in process_output_sum:
                            process_output_sum[key] = 0
                        process_output_sum[key] += int(value)
            process_time_sum = round(process_time_sum, 2)
            output_string = '; '.join([f'{key}: {value}' for key, value in process_output_sum.items()])
            processes_done.loc[
                (processes_done['Process'].str.contains(table)) &
                (processes_done['Process'].str.contains(group)) &
                (~processes_done['Process'].str.contains('_20')), 'Time'
            ] = process_time_sum
            processes_done.loc[
                (processes_done['Process'].str.contains(table)) &
                (processes_done['Process'].str.contains(group)) &
                (~processes_done['Process'].str.contains('_20')), 'Output'
            ] = output_string if output_string else 'True'

    processes_done.to_csv('cleaning_processes_done.csv', index=False)
    
    print('Finished everything.')
    print('========================================')