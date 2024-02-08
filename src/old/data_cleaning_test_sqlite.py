import sqlite3
from multiprocessing import Pool
import pandas as pd
import numpy as np
import dask.dataframe as dd
import time

#TODO Change when using different database
conn = sqlite3.connect('../../db_sqlite/data_test_small.db')
#TODO Change when using different database
database = 'Sqlite_Test_Small'

c = conn.cursor()

c.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = c.fetchall()

processes_done = pd.read_csv('cleaning_processes_done.csv')
processes_done = processes_done[["Process", database]]
processes_done = processes_done.rename(columns={database: 'Done'})
    
rename_deletion_operations = {
    'fahrzeiten_': [
        "DROP COLUMN halt_diva_von",
        "DROP COLUMN halt_punkt_diva_von",
        "DROP COLUMN halt_kurz_von1",
        "DROP COLUMN halt_diva_nach",
        "DROP COLUMN halt_punkt_diva_nach",
        "DROP COLUMN halt_kurz_nach1",
        "RENAME COLUMN ist_an_nach1 TO ist_an_nach",
    ],
    'haltestellen_': [
        "DROP COLUMN halt_ist_aktiv",
    ],
    'passagierfrequenz': [
        "RENAME COLUMN code_codice TO bahnhof_kurz",
        "RENAME COLUMN bahnhof_gare_stazione TO bahnhof_lang",
        "RENAME COLUMN kt_ct_cantone TO kanton",
        "RENAME COLUMN isb_gi TO bahnhofseigner",
        "RENAME COLUMN jahr_anne_anno TO jahr",
        "RENAME COLUMN dtv_tjm_tgm TO durchschnittlicher_täglicher_verkehr",
        "RENAME COLUMN dwv_tmjo_tfm TO durchschnittlicher_werktäglicher_verkehr",
        "RENAME COLUMN dnwv_tmjno_tmgnl TO durchschnittlicher_nicht_werktäglicher_verkehr",
        "RENAME COLUMN evu_ef_itf TO einbezogene_bahnunternehmen",
    ],
    'haltepunkte_': [
        "RENAME COLUMN halt_punkt_ist_aktiv TO ist_aktiv",
        "RENAME COLUMN GPS_Latitude TO latitude",
        "RENAME COLUMN GPS_Longitude TO longitude",
        "RENAME COLUMN GPS_Bearing TO bearing",
    ],
}

for table_tuple in tables:
    start_time = time.time()
    table_name = table_tuple[0].capitalize()
    print('Loading table', table_name)
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    current_time = time.time()
    print('Time taken:', current_time - start_time)
    
    # ALLGEMEIN
    # ====================================================================================================
    #! Renaming and deleting columns
    process_step = processes_done.loc[processes_done['Process'] == f'{table_name}_Renaming_And_Deletion']['Done'].item()
    if np.isnan(process_step):
        current_time = time.time()
        print('Starting deletion and renaming operations for table', table_name)
        for prefix, operations in rename_deletion_operations.items():
            if table_name.startswith(prefix):
                for operation in operations:
                    operation_type, column_name = operation.split(' ')[0], operation.split(' ')[-1]
                    c.execute(f"PRAGMA table_info({table_name})")
                    columns = [column[1] for column in c.fetchall()]
                    if column_name in columns:
                        c.execute(f"ALTER TABLE {table_name} {operation}")
        conn.commit()
        current_time_2 = time.time()
        time_taken = current_time_2 - current_time
        time_taken = round(time_taken, 2)
        processes_done.loc[processes_done['Process'] == f'{table_name}_Renaming_And_Deletion', 'Done'] = str((time_taken, np.nan))
        print('Time taken:', time_taken)
        print('----------------------------------------')
            
    #! Checking for duplicate rows
    process_step = processes_done.loc[processes_done['Process'] == f'{table_name}_Deleting_Duplicate_Rows']['Done'].item()
    if np.isnan(process_step):
        current_time = time.time()
        print('Checking for duplicate rows in table', table_name)
        duplicate_rows = df[df.duplicated()]
        rows_deleted = len(duplicate_rows)
        if rows_deleted > 0:
            print(f'Found {rows_deleted} duplicate rows')
            df.drop_duplicates(inplace=True)
            df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.commit()
        current_time_2 = time.time()
        time_taken = current_time_2 - current_time
        time_taken = round(time_taken, 2)
        processes_done.loc[processes_done['Process'] == f'{table_name}_Deleting_Duplicate_Rows', 'Done'] = str((time_taken, rows_deleted))
        print('Time taken:', time_taken)
        print('----------------------------------------')
    
# NOT APPROVED

    # # FAHRZEITEN
    # # ====================================================================================================
    # if table_name.startswith('Fahrzeiten_'):
    #     pass
    
    # # HALTESTELLEN
    # # ====================================================================================================
    # if table_name.startswith('Haltestellen_'):
    #     #! Check if all the Haltestellen have a consistent halt_id over all the different years
    #     process_step = processes_done.loc[processes_done['Process'] == f'{table_name}_Checking_Id_Consistent']['Done'].item()
    #     if np.isnan(process_step):
    #         current_time = time.time()
    #         print('Checking if all the Haltestellen have a consistent halt_id over all the different years')
    #         halt_ids = {}
    #         inconsistent_rows = 0
    #         for index, row in df.iterrows():
    #             halt_id = row['halt_id']

    #             if halt_id not in halt_ids:
    #                 halt_ids[halt_id] = row_values
    #             else:
    #                 if not row_values.equals(halt_ids[halt_id]):
    #                     inconsistent_rows += 1
    #         print(f'Found {inconsistent_rows} rows with the same halt_id but different entries')
    #         current_time_2 = time.time()
    #         print('Time taken:', current_time_2 - current_time)
      
    # # HALTEPUNKTE
    # # ====================================================================================================
    # if table_name.startswith('Haltepunkte_'):
    #     #! Checking if all halt_punkt_ids have consistent entries over all the different years
    #     process_step = processes_done.loc[processes_done['Process'] == f'{table_name}_Checking_Id_Consistent']['Done'].item()
    #     if np.isnan(process_step):
    #         current_time = time.time()
    #         print('Checking if all the Haltepunkte have a consistent halt_punkt_id over all the different years')
    #         year = int(table_name.split('_')[-1])
    #         year_to_halt_punkt_ids = {}
    #         halt_punkt_ids_to_values = {}
    #         inconsistent_rows = 0
    #         for index, row in df.iterrows():
    #             halt_punkt_id = row['halt_punkt_id']
    #             row_values = row.drop('ist_aktiv')

    #             if halt_punkt_id not in halt_punkt_ids_to_values:
    #                 halt_punkt_ids_to_values[halt_punkt_id] = row_values
    #             else:
    #                 if not row_values.equals(halt_punkt_ids_to_values[halt_punkt_id]):
    #                     inconsistent_rows += 1
    #         year_to_halt_punkt_ids[year] = halt_punkt_ids_to_values
    #         print(f'Found {inconsistent_rows} rows with the same halt_punkt_id but different entries')
    #         current_time_2 = time.time()
    #         print('Time taken:', current_time_2 - current_time)
        
    #     #! Check if Haltepunkte are at the same exact location
    #     process_step = processes_done.loc[processes_done['Process'] == f'{table_name}_Checking_Location']['Done'].item()
    #     if np.isnan(process_step):
    #         current_time = time.time()
    #         print('Checking if Haltepunkte are at the same exact location')
    #         duplicate_rows = df[df.duplicated(subset=['latitude', 'longitude'])]
    #         duplicate_rows_with_same_bearing = duplicate_rows[duplicate_rows.duplicated(subset=['bearing'])]
    #         print(f'Found {len(duplicate_rows)} Haltepunkte with the same latitude and longitude and of those {len(duplicate_rows_with_same_bearing)} have the same bearing')
    #         current_time_2 = time.time()
    #         print('Time taken:', current_time_2 - current_time)
        
    #     #! Check if there was a Haltepunkt that had ist_aktiv as False and then True in a following year
    #     process_step = processes_done.loc[processes_done['Process'] == f'{table_name}_Checking_Reactivated']['Done'].item()
    #     if np.isnan(process_step):
    #         current_time = time.time()
    #         print('Checking if there was a halt_punkt_id that ist_aktiv was False and then True in a proceeding year')
    #         reactivated_haltepunkte = 0
    #         for index, row in df.iterrows():
    #             halt_punkt_id = row['halt_punkt_id']
    #             ist_aktiv = row['ist_aktiv']
    #             for past_year, halt_punkt_ids in year_to_halt_punkt_ids.items():
    #                 if past_year < year and halt_punkt_id in halt_punkt_ids and not halt_punkt_ids[halt_punkt_id]['ist_aktiv'] and ist_aktiv:
    #                     reactivated_haltepunkte += 1
    #         print(f'Found {reactivated_haltepunkte} Haltepunkte that were reactivated')
    #         current_time_2 = time.time()
    #         print('Time taken:', current_time_2 - current_time)