import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, progress
import time
import os
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker
from pyproj import Proj, transform
import warnings
import numpy as np
import calendar
import re
from babel.dates import format_date

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
            'jahr': 'Int64',
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
        }
    }
    
    cleaning_processes_done = pd.read_csv('cleaning_processes_done.csv')
    
    # HELPER FUNCTIONS
    # ========================================
    
    # Fahrzeiten: Function to put Fahrzeiten in correct years
    def put_fahrzeiten_in_correct_years(dfs, table_name):
        total_wrong_year_rows = 0
        wrong_year_rows_dict = {}
        for year, df in dfs.items():
            wrong_year = df[df['betriebsdatum'].dt.year != year]
            if len(wrong_year.index) > 0:
                wrong_year_rows = len(wrong_year)
                total_wrong_year_rows += wrong_year_rows
                correct_years = wrong_year['betriebsdatum'].dt.year.unique().compute()
                for correct_year in correct_years:
                    if correct_year not in wrong_year_rows_dict:
                        wrong_year_rows_dict[correct_year] = wrong_year[wrong_year['betriebsdatum'].dt.year == correct_year]
                    else:
                        wrong_year_rows_dict[correct_year] = dd.concat([wrong_year_rows_dict[correct_year], wrong_year[wrong_year['betriebsdatum'].dt.year == correct_year]])
                dfs[year] = df[df['betriebsdatum'].dt.year == year]
        for correct_year, wrong_year_df in wrong_year_rows_dict.items():
            if correct_year in dfs:
                dfs[correct_year] = dd.concat([dfs[correct_year], wrong_year_df])
            else:
                dfs[correct_year] = wrong_year_df
        output = f"Rows in Wrong Year: {total_wrong_year_rows}"
        changed = True if total_wrong_year_rows > 0 else False
        return dfs, output, changed
    
    # Fahrzeiten: Function to visualize the number of rows of each day in each year
    def visualize_too_little_rows(dfs, table_name):
        output = '/data/figures/cleaning'
        for year, df in dfs.items():
            df['betriebsdatum'] = dd.to_datetime(df['betriebsdatum'])
            counts = df.groupby('betriebsdatum').size().compute()
            output_dir = f'{output}/{table_name}_{year}'
            os.makedirs(output_dir, exist_ok=True)
            for month, month_counts in counts.groupby(counts.index.month):
                month_name = format_date(month_counts.index[0], 'MMMM', locale='de')
                fig, ax = plt.subplots(figsize=(10, 6))
                month_counts.index = month_counts.index.day
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    sns.barplot(x=month_counts.index, y=month_counts, ax=ax, palette='Set2', zorder=2)
                ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x)))
                plt.title(f'Anzahl an Einträgen pro Tag für {year} - {month_name}')
                plt.xlabel('Tag')
                plt.ylabel('Anzahl an Einträgen')
                plt.grid(axis='y', zorder=1)
                plt.savefig(f'{output_dir}/row_count_{month_name.lower()}.png')
                plt.close()
            fig, ax = plt.subplots(figsize=(20, 6))
            weekdays = counts[counts.index.weekday < 5]
            weekends = counts[counts.index.weekday >= 5]
            sns.scatterplot(x=weekdays.index.dayofyear, y=weekdays, ax=ax, color='blue', zorder=2, label='Wochentage')
            sns.scatterplot(x=weekends.index.dayofyear, y=weekends, ax=ax, color='red', zorder=2, label='Wochenenden')
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x)))
            plt.title(f'Anzahl an Einträgen pro Tag für {year}')
            plt.xlabel('Tag des Jahres')
            plt.ylabel('Anzahl an Einträgen')
            plt.grid(axis='both', zorder=1)
            month_lengths = [31, 29 if calendar.isleap(year) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
            x_ticks = [0] + list(np.cumsum(month_lengths))
            ax.set_xticks(x_ticks)
            ax.set_xticklabels([''] + list(calendar.month_abbr[1:]))
            plt.legend()
            plt.savefig(f'{output_dir}/row_count_year.png')
            plt.close()
        changed = False
        return dfs, output, changed
    
    # Fahrzeiten: Function to visualize the number of rows of each day in each year
    def create_january_plot(counts, new=False):
        plt.figure(figsize=(10, 6))
        palette = sns.color_palette('Set2')
        if not new:
            palette = palette[1:] + palette[:1]
        sns.barplot(x=counts.index, y=counts.values, palette=palette, zorder=2)
        formatter = ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x).replace(",", "."))
        plt.gca().yaxis.set_major_formatter(formatter)
        plt.xlabel('Tag')
        plt.grid(axis='y', zorder=1)
        plt.ylabel('')
        plt.title('Anzahl an Zeilen pro Tag im Januar 2022')
        plt.savefig(f'../figures/cleaning/rows_in_january_2022_{"new" if new else "old"}.png')
        plt.close()
    
    # Fahrzeiten: Function to visualize putting Fahrzeiten in correct years
    def visualize_wrong_year(dfs, table_name):
        fahrzeiten_df = dfs[2022]
        fahrzeiten_df = fahrzeiten_df.drop(fahrzeiten_df.columns.difference(['betriebsdatum']), axis=1)
        fahrzeiten_df = fahrzeiten_df[fahrzeiten_df['betriebsdatum'].dt.month == 1]
        daily_counts = fahrzeiten_df.groupby(fahrzeiten_df['betriebsdatum'].dt.day).size().compute()
        daily_counts_without_first = daily_counts.drop(1)
        create_january_plot(daily_counts_without_first)
        create_january_plot(daily_counts, new=True)
        output = '/data/figures/cleaning'
        changed = False
        return dfs, output, changed

    # Fahrzeiten & Haltepunkte: Function to check if foreign keys reference existing ids
    def check_foreign_keys(dfs, table_name):
        invalid_ids = 0
        if table_name == 'fahrzeiten':
            haltepunkte = pd.read_csv('../cleaned_data/haltepunkte.csv')
            total_invalid_ids = 0
            for year, fahrzeiten in dfs.items():
                valid_ids = haltepunkte[haltepunkte['jahr'] == year]['id'].unique()
                invalid_rows_von = fahrzeiten[~fahrzeiten['halt_punkt_id_von'].isin(valid_ids)]
                invalid_rows_nach = fahrzeiten[~fahrzeiten['halt_punkt_id_nach'].isin(valid_ids)]
                invalid_ids = len(invalid_rows_von) + len(invalid_rows_nach)
                total_invalid_ids += invalid_ids
                if len(invalid_rows_von) > 0:
                    invalid_rows_von.to_csv(f'../invalid_data/invalid_ids_von_{year}.csv', index=False, single_file=True)
                if len(invalid_rows_nach) > 0:
                    invalid_rows_nach.to_csv(f'../invalid_data/invalid_ids_nach_{year}.csv', index=False, single_file=True)
            output = f"Foreign Keys not Existing: {total_invalid_ids}"
        elif table_name == 'haltepunkte':
            haltestellen = pd.read_csv('../cleaned_data/haltestellen.csv')
            haltepunkte = dfs
            valid_ids = haltestellen['id'].unique()
            invalid_ids = len(haltepunkte[~haltepunkte['halt_id'].isin(valid_ids)].compute())
            output = f"Foreign Keys not Existing: {invalid_ids}"
        changed = False
        return dfs, output, changed
    
    # Haltestellen: Function to visualize empty cells
    def visualize_empty_cells_haltestellen(df, new=True):
        empty_cells_percentages = {column: (sum_val := df[column].replace('', np.nan).isnull().sum(), sum_val / len(df)) for column in df.columns}
        df_empty_cells = pd.DataFrame(empty_cells_percentages, index=['Empty Cells', 'Percentage']).T
        for column in ['latitude', 'longitude']:
            if new:
                df_empty_cells.loc[column, 'Empty Cells'] += 7
                df_empty_cells.loc[column, 'Percentage'] = df_empty_cells.loc[column, 'Empty Cells'] / len(df)
        df_empty_cells = df_empty_cells.sort_values('Percentage', ascending=True)
        plt.figure(figsize=(10, 6))
        sns.barplot(x=df_empty_cells['Percentage'] * 100, y=df_empty_cells.index, palette='Set2', zorder=2)
        plt.title('Leere Einträge in jeder Spalte in Haltestellen CSV')
        plt.ylabel('')
        plt.xlabel('Prozent (%)')
        plt.grid(axis='x', zorder=1)
        plt.xlim(0, 100)
        plt.xticks(np.arange(0, 101, 5))
        for i, (num, perc) in enumerate(zip(df_empty_cells['Empty Cells'], df_empty_cells['Percentage'] * 100)):
            if num > 0:
                plt.text(perc, i, int(num), va='center')
        plt.savefig('../figures/cleaning/empty_cells_percentage_haltestellen'  + ('_new' if new else '_old') + '.png', bbox_inches='tight')
        plt.close()
        if not new:
            df = df.drop(columns=['latitude', 'longitude'])
            visualize_empty_cells_haltestellen(df, False)
    
    # Haltepunkte: Function to visualize empty cells
    def visualize_empty_cells_haltepunkte(df, new=True):
        empty_cells_percentages = {column: ((sum_val := df[column].replace('', np.nan).isnull().sum()), sum_val / len(df), np.nan, np.nan) for column in df.columns if column != 'bearing'}
        bearing_not_empty = df[df[['latitude', 'longitude']].notna().all(axis=1)]['bearing'].isnull().sum()
        bearing_empty = df[df[['latitude', 'longitude']].isna().any(axis=1)]['bearing'].isnull().sum()
        empty_cells_percentages['bearing'] = (bearing_not_empty + bearing_empty, (bearing_not_empty + bearing_empty) / len(df), bearing_not_empty, bearing_empty)
        df_empty_cells = pd.DataFrame(empty_cells_percentages, index=['Empty Cells', 'Percentage', 'Bearing Not Empty', 'Bearing Empty']).T
        df_empty_cells = df_empty_cells.sort_values('Percentage', ascending=False)
        plt.figure(figsize=(10, 6))
        colors = sns.color_palette('Set2')
        plt.barh('bearing', df_empty_cells.loc['bearing', 'Bearing Empty'] * 100 / len(df), color=colors[0], zorder=2)
        plt.barh('bearing', df_empty_cells.loc['bearing', 'Bearing Not Empty'] * 100 / len(df), left=df_empty_cells.loc['bearing', 'Bearing Empty'] * 100 / len(df), color=colors[1], zorder=2)
        if not new:
            plt.text(df_empty_cells.loc['bearing', 'Percentage'] * 100, 0, '19401', va='center')
            plt.text(df_empty_cells.loc['bearing', 'Bearing Empty'] * 100 / len(df), 0, '7180', va='center')
        for i, column in enumerate(df_empty_cells.index if new else df_empty_cells.index[1:], start=0 if new else 1):
            plt.barh(column, df_empty_cells.loc[column, 'Percentage'] * 100, color=colors[0], zorder=2)
            if df_empty_cells.loc[column, 'Empty Cells'] > 0:
                plt.text(df_empty_cells.loc[column, 'Percentage'] * 100, i, int(df_empty_cells.loc[column, 'Empty Cells']), va='center')
        plt.title('Leere Einträge in jeder Spalte in Haltepunkte CSV')
        plt.ylabel('')
        plt.xlabel('Prozent (%)')
        plt.grid(axis='x', zorder=1)
        plt.xlim(0, 100)
        plt.xticks(np.arange(0, 101, 5))
        plt.savefig('../figures/cleaning/empty_cells_percentage_haltepunkte'  + ('_new' if new else '_old') + '.png', bbox_inches='tight')
        plt.close()
        if not new:
            df.loc[df['bearing'] == -2.0, ['latitude', 'longitude', 'bearing']] = np.nan
            df.loc[df['bearing'] == -1.0, 'bearing'] = np.nan
            visualize_empty_cells_haltepunkte(df, False)
    
    # Passagierfrequenz: Function to visualize empty cells
    def visualize_empty_cells_passagierfrequenz(df, new=True):
        df = df.rename(columns={'durchschnittlicher_täglicher_verkehr': 'durchschnittlicher_\ntäglicher_verkehr', 'durchschnittlicher_werktäglicher_verkehr': 'durchschnittlicher_\nwerktäglicher_verkehr', 'durchschnittlicher_nicht_werktäglicher_verkehr': 'durchschnittlicher_nicht_\nwerktäglicher_verkehr', 'einbezogene_bahnunternehmen': 'einbezogene_\nbahnunternehmen'})
        empty_cells_percentages = {column: (sum_val := df[column].replace('', np.nan).isnull().sum(), sum_val / len(df)) for column in df.columns}
        df_empty_cells = pd.DataFrame(empty_cells_percentages, index=['Empty Cells', 'Percentage']).T
        df_empty_cells = df_empty_cells.sort_values('Percentage', ascending=True)
        plt.figure(figsize=(12, 8))
        if new:
            sns.barplot(x=df_empty_cells['Percentage'] * 100, y=df_empty_cells.index, color=sns.color_palette('Set2')[5], zorder=2)
        else:
            sns.barplot(x=df_empty_cells['Percentage'] * 100, y=df_empty_cells.index, palette='Set2', zorder=2)
        plt.title('Leere Einträge in jeder Spalte in Passagierfrequenz CSV')
        plt.ylabel('')
        plt.xlabel('Prozent (%)')
        plt.grid(axis='x', zorder=1)
        plt.xlim(0, 100)
        plt.xticks(np.arange(0, 101, 5))
        for i, (num, perc) in enumerate(zip(df_empty_cells['Empty Cells'], df_empty_cells['Percentage'] * 100)):
            if num > 0:
                plt.text(perc, i, int(num), va='center')
        plt.savefig('../figures/cleaning/empty_cells_percentage_passagierfrequenz'  + ('_new' if new else '_old') + '.png', bbox_inches='tight')
        plt.close()
        if new:
            df = pd.read_csv('../raw_data/passagierfrequenz.csv', dtype=dtypes['passagierfrequenz'], sep=';')
            df = df.drop(columns=['remarques', 'note', 'remarks'])
            df = df.rename(columns={'code_codice': 'bahnhof_kurz', 'bahnhof_gare_stazione': 'bahnhof_lang', 'kt_ct_cantone': 'kanton', 'isb_gi': 'bahnhofseigner', 'jahr_annee_anno': 'jahr', 'dtv_tjm_tgm': 'durchschnittlicher_\ntäglicher_verkehr', 'dwv_tmjo_tfm': 'durchschnittlicher_\nwerktäglicher_verkehr', 'dnwv_tmjno_tmgnl': 'durchschnittlicher_nicht_\nwerktäglicher_verkehr', 'evu_ef_itf': 'einbezogene_\nbahnunternehmen'})
            df[['latitude', 'longitude']] = df['geopos'].str.split(', ', n=1, expand=True)
            df['link'] = df['lod']
            df = df.drop(columns=['geopos', 'lod'])
            visualize_empty_cells_passagierfrequenz(df, False)
    
    # Haltestellen, Haltepunkte & Passagierfrequenz: Function to visualize empty cells
    def visualize_empty_cells(df, table_name):
        temp_df = df.compute()
        if table_name == 'haltestellen':
            visualize_empty_cells_haltestellen(temp_df)
        elif table_name == 'haltepunkte':
            visualize_empty_cells_haltepunkte(temp_df)
        elif table_name == 'passagierfrequenz':
            visualize_empty_cells_passagierfrequenz(temp_df)
        changed = False
        output = '/data/figures/cleaning'
        return df, output, changed
    
    # Haltestellen & Haltepunkte: Function to check if ids are consistent
    def check_consistent_ids(dfs, table_name):
        inconsistent_columns = {}
        years = sorted(dfs.keys())
        for i in range(len(years) - 1):
            df1 = dfs[years[i]]
            df2 = dfs[years[i + 1]]
            if 'ist_aktiv' in df1.columns:
                df1 = df1.drop('ist_aktiv', axis=1)
            if 'ist_aktiv' in df2.columns:
                df2 = df2.drop('ist_aktiv', axis=1)
            merged = df1.merge(df2, on='id', suffixes=('_x', '_y'))
            for column in df1.columns:
                if column != 'id':
                    inconsistent = (merged[column + '_x'] != merged[column + '_y']) & (merged[column + '_x'].notnull() | merged[column + '_y'].notnull())
                    inconsistent_sum = inconsistent.sum().compute()
                    if inconsistent_sum > 0:
                        if column in inconsistent_columns:
                            inconsistent_columns[column] += inconsistent_sum
                        else:
                            inconsistent_columns[column] = inconsistent_sum
        inconsistent_columns_str = "; ".join(f"{k}: {v}" for k, v in inconsistent_columns.items())
        inconsistent_ids = sum(inconsistent_columns.values())
        if inconsistent_ids == 0:
            output = f"Inconsistent Ids: {inconsistent_ids}"
        else:
            output = f"Inconsistent Ids: {inconsistent_ids}; Columns: {inconsistent_columns_str}"
        changed = False
        return dfs, output, changed
    
    # Haltestellen & Haltepunkte: Function to check if ids are all in last year
    def check_ids_in_last_year(dfs, table_name):
        ids_not_in_last_year = 0
        for year in range(2016, 2022):
            df1 = dfs[year]
            df2 = dfs[year + 1]
            ids_not_in_last_year += len(df1[~df1['id'].isin(df2['id'].compute())])
        if ids_not_in_last_year == 0:
            output = 'True'
        else:
            output = 'False'
        changed = False
        return dfs, output, changed
    
    # Haltestellen & Haltepunkte: Helper Funnction to check if csv files can be merged
    def check_if_mergable(dfs, table_name):
        process = cleaning_processes_done.loc[cleaning_processes_done['Process'] == f'{table_name.capitalize()}_Csv_Merging']
        inconsistent_ids = cleaning_processes_done.loc[cleaning_processes_done['Process'] == f'{table_name.capitalize()}_Checking_Consistent_Ids']['Output'].item()
        inconsistent_ids_found = True if 'id' in inconsistent_ids or 'diva' in inconsistent_ids else False
        ids_in_last_year = cleaning_processes_done.loc[cleaning_processes_done['Process'] == f'{table_name.capitalize()}_Checking_Ids_In_Last_Year']['Output'].item()
        output = True if pd.isnull(process['Time'].item()) and inconsistent_ids_found == False and ids_in_last_year == 'True' else False
        return output
    
    # Haltestellen & Haltepunkte: Helper Function to convert coordinates
    def convert_coordinates(easting, northing):
        swiss_projection = Proj(init='epsg:2056')
        proj_wgs84 = Proj(init='epsg:4326')
        longitude, latitude = transform(swiss_projection, proj_wgs84, easting, northing)
        return latitude, longitude
    
    # Haltestellen & Haltepunkte: Function to add coordinates to Haltestellen or Haltepunkte
    def add_coordinates(df, table_name):
        if table_name == 'haltestellen':
            haltestellen_liste = pd.read_csv('../raw_data/haltestellen_liste.csv', dtype=dtypes['haltestellen_liste'], usecols=dtypes['haltestellen_liste'].keys(), sep=';')
            haltestellen = df.compute()
            haltestellen_liste['E-Koord.'] = haltestellen_liste['E-Koord.'].str.replace('.', '')
            haltestellen_liste['N-Koord.'] = haltestellen_liste['N-Koord.'].str.replace('.', '')
            haltestellen['halt_lang'] = haltestellen['halt_lang'].str.replace(';', ',')
            for index, row in haltestellen.iterrows():
                coordinates = haltestellen_liste[haltestellen_liste['Name'] == row['halt_lang']]
                if len(coordinates.index) > 0:
                    easting = coordinates['E-Koord.'].item()
                    northing = coordinates['N-Koord.'].item()
                    latitude, longitude = convert_coordinates(easting, northing)
                    haltestellen.loc[index, 'latitude'] = latitude
                    haltestellen.loc[index, 'longitude'] = longitude
            haltestellen['halt_lang'] = haltestellen['halt_lang'].str.replace(',', ';')
            haltestellen['halt_lang'] = haltestellen['halt_lang'].str.replace('"', '')
            df = dd.from_pandas(haltestellen, npartitions=10)
            empty_count = df.isnull().sum().sum().compute()
            output = f'Empty Cells: {empty_count}'
        elif table_name == 'haltepunkte':
            haltestellen = pd.read_csv('../cleaned_data/haltestellen.csv')
            haltepunkte = df.compute()
            for index, row in haltepunkte.iterrows():
                if pd.isnull(row['latitude']):
                    halt_id = row['halt_id']
                    halt = haltestellen[haltestellen['id'] == halt_id]
                    if len(halt.index) > 0:
                        latitude = halt['latitude'].item()
                        longitude = halt['longitude'].item()
                        haltepunkte.loc[index, 'latitude'] = latitude
                        haltepunkte.loc[index, 'longitude'] = longitude
                        haltepunkte.loc[index, 'bearing'] = -2
            df = dd.from_pandas(haltepunkte, npartitions=10)
            empty_count = df.isnull().sum().sum().compute()
            output = f'Empty Cells: {empty_count}'
        changed = True
        return df, output, changed
    
    # Haltepunkte: Function to check if Haltepunkte were reactivated
    def check_haltepunkte_reactivated(df, table_name):
        reactivated_haltepunkte = 0
        for year in range(2016, 2022):
            df1 = df[df['jahr'] == year]
            df2 = df[df['jahr'] == year + 1]
            reactivated = df1[df1['ist_aktiv'] == False].merge(df2[df2['ist_aktiv'] == True], on='id')
            if len(reactivated.index) != 0:
                reactivated_haltepunkte += len(reactivated)
        output = f"Reactivated: {reactivated_haltepunkte}"
        changed = False
        return df, output, changed
    
    # Haltepunkte: Function to check if Haltepunkte have the same location
    def check_haltepunkte_location(df, table_name):
        temp_df = df.compute()
        temp_df.dropna(subset=['latitude', 'longitude'], inplace=True)
        same_location = temp_df[temp_df.duplicated(subset=['jahr', 'latitude', 'longitude'], keep=False)]
        same_location_with_same_bearing = temp_df[temp_df.duplicated(subset=['jahr', 'latitude', 'longitude', 'bearing'], keep=False)]
        output = f"Same Location: {len(same_location)}; Also Same Bearing: {len(same_location_with_same_bearing)}"
        changed = False
        return df, output, changed
    
    # Haltepunkte: Function to add bearing value to Haltepunkte, which have latitude and longitude but no bearing
    def add_bearing(df, table_name):
        condition = (df['latitude'].notnull()) & (df['longitude'].notnull()) & (df['bearing'].isnull())
        bearing_series = dd.from_array(np.where(condition, -1, df['bearing'].values))
        df = df.assign(bearing=bearing_series)
        count = df[df['bearing'] == -1].shape[0].compute()
        output = f"Added Bearing: {count}"
        return df, output, True
    
    # Passagierfrequenz: Function to fill missing values
    def fill_values(df, table_name):
        fill_values = {
            'jahr': {
                8503000: 2018,
            },
            'latitude': {
                8503421: 47.654404,
                8509369: 46.21516,
                8503420: 47.654404,
            },
            'longitude': {
                8503421: 8.5733,
                8509369: 10.16675,
                8503420: 8.566823,
            },
            'link': {
                8503421: 'https://lod.opentransportdata.swiss/didok/8503421',
                8509369: 'https://lod.opentransportdata.swiss/didok/8509369',
                8503420: 'https://lod.opentransportdata.swiss/didok/8503420',
            }
        }
        for column, values in fill_values.items():
            for index, value in values.items():
                df[column] = df[column].mask(df['uic'] == index, value)

        count = df.isnull().sum().compute().sum()
        output = f"Empty Cells: {count}"
        return df, output, True
    
    # PROCESSING FUNCTIONS
    # ========================================
    
    # Function to process the data with the above functions in batches
    def process_data_batch(functions, dfs, table_name):
        done_something = False
        changed_something = False
        for function, process_name in functions:
            process = cleaning_processes_done.loc[cleaning_processes_done['Process'] == process_name]
            if pd.isnull(process['Time'].item()):
                done_something = True
                print(f"Working on {process_name.replace('_', ' ')}...")
                start_time = time.time()
                dfs, output, changed = function(dfs, table_name)
                changed_something = changed_something or changed
                current_time = time.time()
                time_taken = round(current_time - start_time, 2)
                cleaning_processes_done.loc[cleaning_processes_done['Process'] == process_name, 'Time'] = time_taken
                cleaning_processes_done.loc[cleaning_processes_done['Process'] == process_name, 'Output'] = output
                print(f"Finished {process_name.replace('_', ' ')} in {time_taken} seconds.")
        if done_something:
            cleaning_processes_done.to_csv('cleaning_processes_done.csv', index=False)
            if changed_something:
                start_time = time.time()
                print(f'Writing {table_name.capitalize()} to csv...')
                if table_name.startswith('fahrzeiten'):
                    for year, df in dfs.items():
                        df.to_csv(f'../cleaned_data/{table_name}_{year}.csv', index=False, single_file=False)
                    #! Add this back in for testing
                    # for year, df in dfs.items():
                    #     df.to_csv(f'../test_data/{table_name}_{year}.csv', index=False, single_file=False)
                else:
                    df.to_csv(f'../cleaned_data/{table_name}.csv', index=False, single_file=True)
                current_time = time.time()
                time_taken = round(current_time - start_time, 2)
                print(f'Finished writing {table_name.capitalize()} to csv in {time_taken} seconds.')
                print('----------------------------------------')
            else:
                print(f'No changes made to {table_name.capitalize()}.')
                print('----------------------------------------')
    
    # Function to process the data with the above functions
    def process_data(functions, df, table_name):
        done_something = False
        changed_something = False
        for function, process_name in functions:
            process = cleaning_processes_done.loc[cleaning_processes_done['Process'] == process_name]
            if pd.isnull(process['Time'].item()):
                done_something = True
                print(f"Working on {process_name.replace('_', ' ')}...")
                start_time = time.time()
                df, output, changed = function(df, table_name)
                changed_something = changed_something or changed
                current_time = time.time()
                time_taken = round(current_time - start_time, 2)
                cleaning_processes_done.loc[cleaning_processes_done['Process'] == process_name, 'Time'] = time_taken
                cleaning_processes_done.loc[cleaning_processes_done['Process'] == process_name, 'Output'] = output
                print(f"Finished {process_name.replace('_', ' ')} in {time_taken} seconds.")
        if done_something:
            cleaning_processes_done.to_csv('cleaning_processes_done.csv', index=False)
            if changed_something:
                start_time = time.time()
                print(f'Writing {table_name.capitalize()} to csv...')
                if table_name.startswith('fahrzeiten'):
                    df.to_csv(f'../cleaned_data/{table_name}.csv', index=False, single_file=False)
                else:
                    df.to_csv(f'../cleaned_data/{table_name}.csv', index=False, single_file=True)
                current_time = time.time()
                time_taken = round(current_time - start_time, 2)
                print(f'Finished writing {table_name.capitalize()} to csv in {time_taken} seconds.')
                print('----------------------------------------')
            else:
                print(f'No changes made to {table_name.capitalize()}.')
                print('----------------------------------------')
    
    # Haltestellen & Haltepunkte: Function to merge csv files
    def merge_csv_files(dfs, table_name):
        mergable = check_if_mergable(dfs, table_name)
        if mergable:
            start_time = time.time()
            if table_name == 'haltestellen':
                print('----------------------------------------')
                print(f'No inconsistent ids found in {table_name.capitalize()}. Csv files will be deleted except for the last year.')
                for year in range(2016, 2023):
                    if year != 2022:
                        os.remove(f'../cleaned_data/{table_name}_{year}.csv')
                os.rename(f'../cleaned_data/{table_name}_2022.csv', f'../cleaned_data/{table_name}.csv')
            elif table_name == 'haltepunkte':
                print('----------------------------------------')
                print(f'No inconsistent ids found in {table_name.capitalize()}. Csv files will be merged.')
                dfs = []
                for year in range(2016, 2023):
                    df = pd.read_csv(f'../cleaned_data/{table_name}_{year}.csv')
                    df['jahr'] = year
                    dfs.append(df)
                    os.remove(f'../cleaned_data/{table_name}_{year}.csv')
                final_df = pd.concat(dfs)
                cols = ['jahr'] + [col for col in final_df.columns if col != 'jahr']
                final_df = final_df[cols]
                final_df.to_csv(f'../cleaned_data/{table_name}.csv', index=False)
            current_time = time.time()
            time_taken = round(current_time - start_time, 2)
            cleaning_processes_done.loc[cleaning_processes_done['Process'] == f'{table_name.capitalize()}_Csv_Merging', 'Time'] = time_taken
            cleaning_processes_done.loc[cleaning_processes_done['Process'] == f'{table_name.capitalize()}_Csv_Merging', 'Output'] = 'True'
            cleaning_processes_done.to_csv('cleaning_processes_done.csv', index=False)
            print(f'Finished merging csv files of {table_name.capitalize()} in {time_taken} seconds.')
            print('----------------------------------------')
        else:
            print(f'Inconsistent ids found in {table_name.capitalize()}. Csv files will not be merged.')
            print('----------------------------------------')
    
    # Passagierfrequenz
    # ========================================
    print('Working on Passagierfrequenz...')
    print('----------------------------------------')
    passagierfrequenz_df = dd.read_csv('../cleaned_data/passagierfrequenz.csv', assume_missing=True, dtype=dtypes['passagierfrequenz'])
    
    functions = [
        (fill_values, 'Passagierfrequenz_Filling_Values'),
        (visualize_empty_cells, 'Visualizing_Passagierfrequenz_Empty_Cells'),
    ]
    
    process_data(functions, passagierfrequenz_df, 'passagierfrequenz')
    
    print('Finished Passagierfrequenz.')
    print('========================================')
    
    # Haltestellen
    # ========================================
    print('Working on Haltestellen...')
    print('----------------------------------------')
    if not os.path.exists('../cleaned_data/haltestellen.csv'):
        haltestellen_dfs = {year: dd.read_csv(f'../cleaned_data/haltestellen_{year}.csv') for year in range(2016, 2023)}
    
        functions = [
            (check_consistent_ids, 'Haltestellen_Checking_Consistent_Ids'),
            (check_ids_in_last_year, 'Haltestellen_Checking_Ids_In_Last_Year'),
        ]
    
        process_data_batch(functions, haltestellen_dfs, 'haltestellen')
        
        merge_csv_files(haltestellen_dfs, 'haltestellen')
        
        del haltestellen_dfs
    
    haltetstellen_df = dd.read_csv('../cleaned_data/haltestellen.csv', assume_missing=True, dtype=dtypes['haltestellen'])
    
    functions = [
        (add_coordinates, 'Haltestellen_Adding_Coordinates'),
        (visualize_empty_cells, 'Visualizing_Haltestellen_Empty_Cells'),
    ]
    
    process_data(functions, haltetstellen_df, 'haltestellen')
    
    print('Finished Haltestellen.')
    print('========================================')
    
    # Haltepunkte
    # ========================================
    print('Working on Haltepunkte...')
    print('----------------------------------------')
    if not os.path.exists('../cleaned_data/haltepunkte.csv'):
        haltepunkte_dfs = {year: dd.read_csv(f'../cleaned_data/haltepunkte_{year}.csv') for year in range(2016, 2023)}
    
        functions = [
            (check_consistent_ids, 'Haltepunkte_Checking_Consistent_Ids'),
            (check_ids_in_last_year, 'Haltepunkte_Checking_Ids_In_Last_Year'),
        ]
        
        process_data_batch(functions, haltepunkte_dfs, 'haltepunkte')
    
        merge_csv_files(haltepunkte_dfs, 'haltepunkte')
        
        del haltepunkte_dfs
    
    haltepunkte_df = dd.read_csv('../cleaned_data/haltepunkte.csv', assume_missing=True, dtype=dtypes['haltepunkte'])
    
    functions = [
        (check_haltepunkte_reactivated, 'Haltepunkte_Checking_Reactivated'),
        (check_haltepunkte_location, 'Haltepunkte_Checking_Same_Location'),
        (check_foreign_keys, 'Haltepunkte_Checking_Foreign_Keys'),
        (add_bearing, 'Haltepunkte_Adding_Bearing'),
        (add_coordinates, 'Haltepunkte_Adding_Coordinates'),
        (visualize_empty_cells, 'Visualizing_Haltepunkte_Empty_Cells'),
    ]
    
    process_data(functions, haltepunkte_df, 'haltepunkte')
    
    print('Finished Haltepunkte.')
    print('========================================')
    
    # Fahrzeiten
    # ========================================
    print('Working on Fahrzeiten...')
    print('----------------------------------------')
    fahrzeiten_dfs = {}
    for year in range(2016, 2023):
        file_path = f'../cleaned_data/fahrzeiten_{year}.csv/*.part'
        #! Add this back in for testing
        # file_path = f'../test_data/fahrzeiten_{year}.csv/*.part'
        df = dd.read_csv(file_path, assume_missing=True, dtype=dtypes['fahrzeiten'])
        df['betriebsdatum'] = dd.to_datetime(df['betriebsdatum'], errors='coerce').dt.date
        df['datum_von'] = dd.to_datetime(df['datum_von'], errors='coerce').dt.date
        df['datum_nach'] = dd.to_datetime(df['datum_nach'], errors='coerce').dt.date
        fahrzeiten_dfs[year] = df
    
    functions = [
        (check_foreign_keys, 'Fahrzeiten_Checking_Foreign_Keys'),
        (put_fahrzeiten_in_correct_years, 'Fahrzeiten_Putting_In_Correct_Years'),
        (visualize_too_little_rows, 'Visualizing_Fahrzeiten_Too_Little_Rows'),
        (visualize_wrong_year, 'Visualizing_Fahrzeiten_Putting_In_Correct_Years'),
    ]
    
    process_data_batch(functions, fahrzeiten_dfs, 'fahrzeiten')
    
    print('Finished Fahrzeiten.')
    print('========================================')
    
    print('\n')
    print('Finished Everything.')
    print('========================================')