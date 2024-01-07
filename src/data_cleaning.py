import sqlite3
from multiprocessing import Pool
import pandas as pd

def get_row_without_id(row):
    if type(row[3]) == str:
        new_year = row[3].split('.')[-1]
    else:
        new_year = row[3] - 2000
    print('New year:', new_year)
    # Exclude the id value from the row
    row_without_id = row[1:]
    return new_year, row_without_id

conn = sqlite3.connect('../raw_data/data.db')

c = conn.cursor()

c.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = c.fetchall()

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
    ]
}

for table_tuple in tables:
    table_name = table_tuple[0]
    #! Renaming and deleting columns
    # print('Starting deletion and renaming operations for table', table_name)
    # for prefix, operations in rename_deletion_operations.items():
    #     if table_name.startswith(prefix):
    #         for operation in operations:
    #             c.execute(f"ALTER TABLE {table_name} {operation}")
    #             conn.commit()
    
    #! Adding an id column to the fahrzeiten tables
    print('Starting id column addition for table', table_name)
    # Load the data into a DataFrame
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

    # Add an ID column
    df.insert(0, 'id', range(1, len(df) + 1))

    # Write the data back to the database
    df.to_sql(f"{table_name}_new", conn, if_exists='replace', index=False)

    # Drop the old table and rename the new one
    c.execute(f"DROP TABLE {table_name}")
    c.execute(f"ALTER TABLE {table_name}_new RENAME TO {table_name}")
    conn.commit()
        
    #! Checking if the year of all rows in a table is from that table's year
    #! If not, move the row to the correct table
    if table_name.startswith('fahrzeiten_'):
        print("Checking if the year of all rows in a table is from that table's year")
        year = table_name.split('_')[-1]
        year_short = year[2:]
        print('Year:', year)

        # Create a temporary table with the rows that need to be moved
        c.execute(f"""
            CREATE TEMPORARY TABLE temp AS
            SELECT * FROM {table_name} WHERE betriebsdatum NOT LIKE '%.{year_short}'
        """)

        # Delete the rows in the original table
        c.execute(f"""
            DELETE FROM {table_name} WHERE id IN (SELECT id FROM temp)
        """)

        # Insert the rows from the temporary table into the correct tables
        c.execute("BEGIN TRANSACTION")
        for row in c.execute("SELECT * FROM temp"):
            new_year = get_row_without_id(row)[0]
            row_without_id = row[1:]
            # Get the maximum ID from the destination table
            c.execute(f"SELECT MAX(id) FROM fahrzeiten_{new_year}")
            max_id = c.fetchone()[0]
            if max_id is None:
                max_id = 0
            # Set the ID for the row to be inserted
            row_id = max_id + 1
            c.execute(f"INSERT INTO fahrzeiten_{new_year} (id, {other_columns}) VALUES ({row_id}, {row_without_id})")
        conn.commit()

        # Drop the temporary table
        c.execute("DROP TABLE temp")