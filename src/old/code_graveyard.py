#! test.py contents:
# =============================================================================

    # FINDING INVALID DATES
    # =====================
    # re = '[0-9]{2}\.[0-9]{2}\.[0-9]{2}'
    # for year in range(2016, 2023):
    #     start_time = time.time()
    #     df = dd.read_csv(f'../../cleaned_data/fahrzeiten_{year}.csv', assume_missing=True, dtype=dtypes, usecols=dtypes.keys())
    #     current_time = time.time()
    #     time_taken = round(current_time - start_time, 2)
    #     print(f'Loaded fahrzeiten_{year}.csv in {time_taken} seconds')
    #     start_time = time.time()
    #     selected_df = df[df['datum_von'].str.match(re) == False]['datum_von'].compute()
    #     current_time = time.time()
    #     time_taken = round(current_time - start_time, 2)
    #     print(f'Computed datum_von in {time_taken} seconds')
    #     print('------------------------------------------')
    #     print(selected_df)
    #     start_time = time.time()
    #     selected_df = df[df['datum_nach'].str.match(re) == False]['datum_nach'].compute()
    #     current_time = time.time()
    #     time_taken = round(current_time - start_time, 2)
    #     print(f'Computed datum_nach in {time_taken} seconds')
    #     print('------------------------------------------')
    #     print(selected_df)
    
    # TESTING LOADING TIMES
    # =====================
    # dtypes = {
    #     'linie': 'Int16',
    #     'richtung': 'Int8',
    #     'betriebsdatum': 'object',
    #     'fahrzeug': 'Int32',
    #     'kurs': 'Int16',
    #     'seq_von': 'Int32',
    #     'halt_diva_von': 'Int32',
    #     'halt_punkt_diva_von': 'Int32',
    #     'halt_kurz_von1': 'string',
    #     'datum_von': 'string',
    #     'soll_an_von': 'Int32',
    #     'ist_an_von': 'Int32',
    #     'soll_ab_von': 'Int32',
    #     'ist_ab_von': 'Int32',
    #     'seq_nach': 'Int32',
    #     'halt_diva_nach': 'Int32',
    #     'halt_punkt_diva_nach': 'Int32',
    #     'halt_kurz_nach1': 'string',
    #     'datum_nach': 'string',
    #     'soll_an_nach': 'Int32',
    #     'ist_an_nach1': 'Int32',
    #     'soll_ab_nach': 'Int32',
    #     'ist_ab_nach': 'Int32',
    #     'fahrt_id': 'Int64',
    #     'fahrweg_id': 'Int64',
    #     'fw_no': 'Int16',
    #     'fw_typ': 'Int8',
    #     'fw_kurz': 'string',
    #     'fw_lang': 'string',
    #     'umlauf_von': 'Int64',
    #     'halt_id_von': 'Int64',
    #     'halt_id_nach': 'Int64',
    #     'halt_punkt_id_von': 'Int64',
    #     'halt_punkt_id_nach': 'Int64'
    # }
    # fahrzeiten_files = glob.glob('../../raw_data/fahrzeiten_2016/fahrzeiten_*.csv')
    # # LOADING FILES
    # # -------------
    # start_time = time.time()
    # df = dd.read_csv(fahrzeiten_files, assume_missing=True, dtype=dtypes, usecols=dtypes.keys())
    # current_time = time.time()
    # time_taken = round(current_time - start_time, 2)
    # print(f'Loaded all files in {time_taken} seconds')
    # # WRITING FILES
    # # -------------
    # start_time = time.time()
    # df.to_csv('../../fahrzeiten_2016_single_file.csv', single_file=True, index=False)
    # current_time = time.time()
    # time_taken = round(current_time - start_time, 2)
    # print(f'Wrote single_file=True in {time_taken} seconds')
    # start_time = time.time()
    # df.to_csv('../../fahrzeiten_2016_multiple_files.csv', single_file=False, index=False)
    # current_time = time.time()
    # time_taken = round(current_time - start_time, 2)
    # print(f'Wrote single_file=False in {time_taken} seconds')
    # # LOADING NEW FILES
    # # -------------------
    # start_time = time.time()
    # df = dd.read_csv('../../fahrzeiten_2016_single_file.csv', assume_missing=True, dtype=dtypes, usecols=dtypes.keys())
    # current_time = time.time()
    # time_taken = round(current_time - start_time, 2)
    # print(f'Loaded single_file=True in {time_taken} seconds')
    # start_time = time.time()
    # df = dd.read_csv('../../fahrzeiten_2016_multiple_files.csv/*', assume_missing=True, dtype=dtypes, usecols=dtypes.keys())
    # current_time = time.time()
    # time_taken = round(current_time - start_time, 2)
    # print(f'Loaded single_file=False in {time_taken} seconds')
    
    # FINDING OUTLIERS IN ROWCOUNTS IN FAHRZEITEN
    # ===========================================
    # pd.set_option('display.max_rows', 10000)
    # with open('output.txt', 'w') as f:
    #     for year in range(2016, 2023):
    #         df = dd.read_csv(f'../../cleaned_data/fahrzeiten_{year}.csv/*', assume_missing=True, dtype=dtypes['fahrzeiten'])
    #         print(f'Year: {year}')
    #         f.write(f'Year: {year}\n')
    #         f.write('------------------------------------------\n')
    #         groupby_result = df.groupby('betriebsdatum').size().compute()
    #         sorted_result = groupby_result.sort_index()
    #         f.write(str(sorted_result))
    #         f.write('\n------------------------------------------\n')
    #         print('------------------------------------------')
    
    
    
#! tony.py contents:
# =============================================================================

# import pandas as pd
# from pyproj import Proj, transform
# import warnings

# def convert_coordinates(easting, northing):
#     swiss_projection = Proj(init='epsg:2056')
#     proj_wgs84 = Proj(init='epsg:4326')
#     longitude, latitude = transform(swiss_projection, proj_wgs84, easting, northing)
#     return latitude, longitude

# def update_coordinates(df):
#     changed = False
#     for index, row in df.iterrows():
#         if pd.isnull(row['latitude']):
#             print(f"Coordinates missing for: {row['halt_lang']}")
#             action = input("Enter 'e' to exit, 's' to skip, or any other key to update: ")
#             if action.lower() == 'e':
#                 print('-------------------------------------------------------------------')
#                 return df, changed
#             elif action.lower() == 's':
#                 print('-------------------------------------------------------------------')
#                 continue
#             else:
#                 easting = input("Enter E-Koord: ")
#                 northing = input("Enter N-Koord: ")
#                 latitude, longitude = convert_coordinates(easting, northing)
#                 print(f"Latitude: {latitude}")
#                 print(f"Longitude: {longitude}")
#                 df.loc[index, 'latitude'] = latitude
#                 df.loc[index, 'longitude'] = longitude
#                 print('Do you want to change the name of the Haltestelle, currently: ' + row['halt_lang'])
#                 action = input("Enter 'y' to change, or any other key to skip: ")
#                 if action.lower() == 'y':
#                     halt_lang = input("Enter new name: ")
#                     df.loc[index, 'halt_lang'] = halt_lang
#                     print(f"Name changed to: {halt_lang}")
#                     print('Do you want to change the halt_kurz of the Haltestelle, currently: ' + row['halt_kurz'])
#                     action = input("Enter 'y' to change, or any other key to skip: ")
#                     if action.lower() == 'y':
#                         halt_kurz = input("Enter new halt_kurz: ")
#                         df.loc[index, 'halt_kurz'] = halt_kurz
#                         print(f"halt_kurz changed to: {halt_kurz}")
#                 print('-------------------------------------------------------------------')
#                 changed = True
#     return df, changed

# def main():
    
#     warnings.simplefilter(action='ignore', category=FutureWarning)
    
#     dtypes = {
#         'haltestellen': {
#             'id': 'Int64',
#             'diva': 'Int64',
#             'halt_kurz': 'string',
#             'halt_lang': 'string'
#         }
#     }
#     haltestellen_df = pd.read_csv('../../cleaned_data/haltestellen.csv', dtype=dtypes['haltestellen'])
#     haltestellen_df, updated = update_coordinates(haltestellen_df)
#     if updated:
#         save = input("Do you want to save changes? (y/n): ")
#         if save.lower() == 'y':
#             haltestellen_df.to_csv('../../cleaned_data/haltestellen.csv', index=False)
#             print("Changes saved.")
#         else:
#             print("Changes not saved.")
#     else:
#         print("No changes made.")
#     print('===================================================================\n')
    
#     haltestellen_df = pd.read_csv('../../cleaned_data/haltestellen.csv', dtype=dtypes['haltestellen'])
#     print(f"Coordinates still missing for: {haltestellen_df['latitude'].isnull().sum()} Haltestellen.")
    
#     haltestellen_df['halt_lang'] = haltestellen_df['halt_lang'].str.replace(',', ';')
#     haltestellen_df['halt_lang'] = haltestellen_df['halt_lang'].str.replace('"', '')
#     haltestellen_df.to_csv('../../cleaned_data/haltestellen.csv', index=False)

# if __name__ == "__main__":
#     main()