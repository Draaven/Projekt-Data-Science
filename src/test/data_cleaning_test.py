import sqlite3
from multiprocessing import Pool
import pandas as pd
import dask.dataframe as dd
import time

#! For test
conn = sqlite3.connect('../../raw_data/data_test.db')
c = conn.cursor()

c.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = c.fetchall()

processes_done = pd.read_csv('cleaning_processes_done.csv')

# # Print out the table names and the column names
# for table_name in tables:
#     print(table_name[0])
#     c.execute("PRAGMA table_info({})".format(table_name[0]))
#     print([tup[1] for tup in c.fetchall()])
    
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
    table_name = table_tuple[0]
    print('Loading table', table_name)
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    current_time = time.time()
    print('Time taken:', current_time - start_time)
    
    # ALLGEMEIN
    # ====================================================================================================
    #! Renaming and deleting columns
    for prefix, operations in rename_deletion_operations.items():
        if table_name.startswith(prefix):
            current_time = time.time()
            print('Starting deletion and renaming operations for table', table_name)
            for operation in operations:
                operation_type, column_name = operation.split(' ')[0], operation.split(' ')[-1]
                c.execute(f"PRAGMA table_info({table_name})")
                columns = [column[1] for column in c.fetchall()]
                if column_name in columns:
                    c.execute(f"ALTER TABLE {table_name} {operation}")
                    conn.commit()
            current_time_2 = time.time()
            print('Time taken:', current_time_2 - current_time)
            
    #! Checking for duplicate rows
    current_time = time.time()
    print('Checking for duplicate rows in table', table_name)
    duplicate_rows = df[df.duplicated()]
    if len(duplicate_rows) > 0:
        print(f'Found {len(duplicate_rows)} duplicate rows')
        user_input = input('Do you want to delete the duplicate rows? (y/n)')
        if user_input == 'y':
            df.drop_duplicates(inplace=True)
            df.to_sql(table_name, conn, if_exists='replace', index=False)
    current_time_2 = time.time()
    print('Time taken:', current_time_2 - current_time)
    
    # FAHRZEITEN
    # ====================================================================================================
    if table_name.startswith('fahrzeiten_'):
        pass
    
    # HALTESTELLEN
    # ====================================================================================================
    if table_name.startswith('haltestellen_'):
        #! Check if all the Haltestellen have a consistent halt_id over all the different years
        current_time = time.time()
        print('Checking if all the Haltestellen have a consistent halt_id over all the different years')
        halt_ids = {}
        inconsistent_rows = 0
        for index, row in df.iterrows():
            halt_id = row['halt_id']

            if halt_id not in halt_ids:
                halt_ids[halt_id] = row_values
            else:
                if not row_values.equals(halt_ids[halt_id]):
                    inconsistent_rows += 1
        print(f'Found {inconsistent_rows} rows with the same halt_id but different entries')
        current_time_2 = time.time()
        print('Time taken:', current_time_2 - current_time)
      
    # HALTEPUNKTE
    # ====================================================================================================
    if table_name.startswith('haltepunkte_'):
        #! Checking if all halt_punkt_ids have consistent entries over all the different years
        current_time = time.time()
        print('Checking if all the Haltepunkte have a consistent halt_punkt_id over all the different years')
        year = int(table_name.split('_')[-1])
        year_to_halt_punkt_ids = {}
        halt_punkt_ids_to_values = {}
        inconsistent_rows = 0
        for index, row in df.iterrows():
            halt_punkt_id = row['halt_punkt_id']
            row_values = row.drop('ist_aktiv')

            if halt_punkt_id not in halt_punkt_ids_to_values:
                halt_punkt_ids_to_values[halt_punkt_id] = row_values
            else:
                if not row_values.equals(halt_punkt_ids_to_values[halt_punkt_id]):
                    inconsistent_rows += 1
        year_to_halt_punkt_ids[year] = halt_punkt_ids_to_values
        print(f'Found {inconsistent_rows} rows with the same halt_punkt_id but different entries')
        current_time_2 = time.time()
        print('Time taken:', current_time_2 - current_time)
        
        #! Check if Haltepunkte are at the same exact location
        current_time = time.time()
        print('Checking if Haltepunkte are at the same exact location')
        duplicate_rows = df[df.duplicated(subset=['latitude', 'longitude'])]
        duplicate_rows_with_same_bearing = duplicate_rows[duplicate_rows.duplicated(subset=['bearing'])]
        print(f'Found {len(duplicate_rows)} Haltepunkte with the same latitude and longitude and of those {len(duplicate_rows_with_same_bearing)} have the same bearing')
        current_time_2 = time.time()
        print('Time taken:', current_time_2 - current_time)
        
        #! Check if there was a Haltepunkt that had ist_aktiv as False and then True in a following year
        current_time = time.time()
        print('Checking if there was a halt_punkt_id that ist_aktiv was False and then True in a proceeding year')
        reactivated_haltepunkte = 0
        for index, row in df.iterrows():
            halt_punkt_id = row['halt_punkt_id']
            ist_aktiv = row['ist_aktiv']
            for past_year, halt_punkt_ids in year_to_halt_punkt_ids.items():
                if past_year < year and halt_punkt_id in halt_punkt_ids and not halt_punkt_ids[halt_punkt_id]['ist_aktiv'] and ist_aktiv:
                    reactivated_haltepunkte += 1
        print(f'Found {reactivated_haltepunkte} Haltepunkte that were reactivated')
        current_time_2 = time.time()
        print('Time taken:', current_time_2 - current_time)


    # PASSAGIERFREQUENZ
    # ====================================================================================================
    if table_name == 'passagierfrequenz':
        #! Checking if there is a remark, it is in all the different languages
        current_time = time.time()
        print('Checking if there is a remark in Passagierfrequenz it is in all the different languages')
        inconsistent_rows = 0
        for index, row in df.iterrows():
            if pd.notna(row['bemerkungen']) or pd.notna(row['remarques']) or pd.notna(row['note']) or pd.notna(row['remarks']):
                if pd.isna(row['bemerkungen']) or pd.isna(row['remarques']) or pd.isna(row['note']) or pd.isna(row['remarks']):
                    inconsistent_rows += 1

        print(f'Found {inconsistent_rows} rows with inconsistent remarks')
        user_input = input('Do you want to delete the other languages? (y/n)')
        if user_input == 'y':
            df.drop(columns=['remarques', 'note', 'remarks'], inplace=True)
            df.to_sql(table_name, conn, if_exists='replace', index=False)
        current_time_2 = time.time()
        print('Time taken:', current_time_2 - current_time)
        
        #! Splitting the column geopos into latitude and longitude
        current_time = time.time()
        print('Splitting the column geopos into latitude and longitude')
        df['latitude'] = df['geopos'].apply(lambda x: x.split(',')[0])
        df['longitude'] = df['geopos'].apply(lambda x: x.split(',')[1])
        df.drop(columns=['geopos'], inplace=True)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        current_time_2 = time.time()
        print('Time taken:', current_time_2 - current_time)
    
# NOT APPROVED

# OUTSIDE FOR LOOP
# def get_row_without_id(row):
#     if type(row[3]) == str:
#         new_year = row[3].split('.')[-1]
#     else:
#         new_year = row[3] - 2000
#     print('New year:', new_year)
#     row_without_id = row[1:]
#     return new_year, row_without_id

# INSIDE FOR LOOP
# #! Adding an id column to the fahrzeiten tables
# print('Starting id column addition for table', table_name)
# # Load the data into a DataFrame
# df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

# # Add an ID column
# df.insert(0, 'id', range(1, len(df) + 1))

# # Write the data back to the database
# df.to_sql(f"{table_name}_new", conn, if_exists='replace', index=False)

# # Drop the old table and rename the new one
# c.execute(f"DROP TABLE {table_name}")
# c.execute(f"ALTER TABLE {table_name}_new RENAME TO {table_name}")
# conn.commit()
# current_time_2 = time.time()
# print('Time taken:', current_time_2 - current_time)
    
# #! Checking if the year of all rows in a table is from that table's year
# #! If not, move the row to the correct table
# if table_name.startswith('fahrzeiten_'):
#     print("Checking if the year of all rows in a table is from that table's year")
#     year = table_name.split('_')[-1]
#     year_short = year[2:]
#     print('Year:', year)

#     # Create a temporary table with the rows that need to be moved
#     c.execute(f"""
#         CREATE TEMPORARY TABLE temp AS
#         SELECT * FROM {table_name} WHERE betriebsdatum NOT LIKE '%.{year_short}'
#     """)

#     # Delete the rows in the original table
#     c.execute(f"""
#         DELETE FROM {table_name} WHERE id IN (SELECT id FROM temp)
#     """)

#     # Insert the rows from the temporary table into the correct tables
#     c.execute("BEGIN TRANSACTION")
#     for row in c.execute("SELECT * FROM temp"):
#         new_year = get_row_without_id(row)[0]
#         row_without_id = row[1:]
#         # Get the maximum ID from the destination table
#         c.execute(f"SELECT MAX(id) FROM fahrzeiten_{new_year}")
#         max_id = c.fetchone()[0]
#         if max_id is None:
#             max_id = 0
#         # Set the ID for the row to be inserted
#         row_id = max_id + 1
#         c.execute(f"INSERT INTO fahrzeiten_{new_year} (id, {other_columns}) VALUES ({row_id}, {row_without_id})")
#     conn.commit()

#     # Drop the temporary table
#     c.execute("DROP TABLE temp")