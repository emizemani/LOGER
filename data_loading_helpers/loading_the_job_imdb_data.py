import os
import psycopg2
import pandas as pd
import numpy as np

# Database connection parameters
db_params = {
    'dbname': 'imdbload',
    'user': 'postgres',
    'password': 'thesis',  # Replace with your actual password
    'host': 'localhost',
    'port': '5433'
}

# Directory containing the CSV files
csv_directory = '/home/emionatrip/Desktop/thesis/LOGER/imdb_data_job'

# Define table schemas to set correct column names and data types
table_schemas = {
    'aka_name': {
        'columns': ['id', 'person_id', 'name', 'imdb_index', 'name_pcode_cf', 'name_pcode_nf', 'surname_pcode', 'md5sum'],
        'dtypes': {'id': 'Int64', 'person_id': 'Int64', 'name': 'object', 'imdb_index': 'object', 'name_pcode_cf': 'object', 'name_pcode_nf': 'object', 'surname_pcode': 'object', 'md5sum': 'object'}
    },
    'aka_title': {
        'columns': ['id', 'movie_id', 'title', 'imdb_index', 'kind_id', 'production_year', 'phonetic_code', 'episode_of_id', 'season_nr', 'episode_nr', 'note', 'md5sum'],
        'dtypes': {'id': 'Int64', 'movie_id': 'Int64', 'title': 'object', 'imdb_index': 'object', 'kind_id': 'Int64', 'production_year': 'Int64', 'phonetic_code': 'object', 'episode_of_id': 'Int64', 'season_nr': 'Int64', 'episode_nr': 'Int64', 'note': 'object', 'md5sum': 'object'}
    },
    'cast_info': {
        'columns': ['id', 'person_id', 'movie_id', 'person_role_id', 'note', 'nr_order', 'role_id'],
        'dtypes': {'id': 'Int64', 'person_id': 'Int64', 'movie_id': 'Int64', 'person_role_id': 'Int64', 'note': 'object', 'nr_order': 'Int64', 'role_id': 'Int64'}
    },
    'char_name': {
        'columns': ['id', 'name', 'imdb_index', 'imdb_id', 'name_pcode_nf', 'surname_pcode', 'md5sum'],
        'dtypes': {'id': 'Int64', 'name': 'object', 'imdb_index': 'object', 'imdb_id': 'Int64', 'name_pcode_nf': 'object', 'surname_pcode': 'object', 'md5sum': 'object'}
    },
    'comp_cast_type': {
        'columns': ['id', 'kind'],
        'dtypes': {'id': 'Int64', 'kind': 'object'}
    },
    'company_name': {
        'columns': ['id', 'name', 'country_code', 'imdb_id', 'name_pcode_nf', 'name_pcode_sf', 'md5sum'],
        'dtypes': {'id': 'Int64', 'name': 'object', 'country_code': 'object', 'imdb_id': 'Int64', 'name_pcode_nf': 'object', 'name_pcode_sf': 'object', 'md5sum': 'object'}
    },
    'company_type': {
        'columns': ['id', 'kind'],
        'dtypes': {'id': 'Int64', 'kind': 'object'}
    },
    'complete_cast': {
        'columns': ['id', 'movie_id', 'subject_id', 'status_id'],
        'dtypes': {'id': 'Int64', 'movie_id': 'Int64', 'subject_id': 'Int64', 'status_id': 'Int64'}
    },
    'info_type': {
        'columns': ['id', 'info'],
        'dtypes': {'id': 'Int64', 'info': 'object'}
    },
    'keyword': {
        'columns': ['id', 'keyword', 'phonetic_code'],
        'dtypes': {'id': 'Int64', 'keyword': 'object', 'phonetic_code': 'object'}
    },
    'kind_type': {
        'columns': ['id', 'kind'],
        'dtypes': {'id': 'Int64', 'kind': 'object'}
    },
    'link_type': {
        'columns': ['id', 'link'],
        'dtypes': {'id': 'Int64', 'link': 'object'}
    },
    'movie_companies': {
        'columns': ['id', 'movie_id', 'company_id', 'company_type_id', 'note'],
        'dtypes': {'id': 'Int64', 'movie_id': 'Int64', 'company_id': 'Int64', 'company_type_id': 'Int64', 'note': 'object'}
    },
    'movie_info': {
        'columns': ['id', 'movie_id', 'info_type_id', 'info', 'note'],
        'dtypes': {'id': 'Int64', 'movie_id': 'Int64', 'info_type_id': 'Int64', 'info': 'object', 'note': 'object'}
    },
    'movie_info_idx': {
        'columns': ['id', 'movie_id', 'info_type_id', 'info', 'note'],
        'dtypes': {'id': 'Int64', 'movie_id': 'Int64', 'info_type_id': 'Int64', 'info': 'object', 'note': 'object'}
    },
    'movie_keyword': {
        'columns': ['id', 'movie_id', 'keyword_id'],
        'dtypes': {'id': 'Int64', 'movie_id': 'Int64', 'keyword_id': 'Int64'}
    },
    'movie_link': {
        'columns': ['id', 'movie_id', 'linked_movie_id', 'link_type_id'],
        'dtypes': {'id': 'Int64', 'movie_id': 'Int64', 'linked_movie_id': 'Int64', 'link_type_id': 'Int64'}
    },
    'name': {
        'columns': ['id', 'name', 'imdb_index', 'imdb_id', 'gender', 'name_pcode_cf', 'name_pcode_nf', 'surname_pcode', 'md5sum'],
        'dtypes': {'id': 'Int64', 'name': 'object', 'imdb_index': 'object', 'imdb_id': 'Int64', 'gender': 'object', 'name_pcode_cf': 'object', 'name_pcode_nf': 'object', 'surname_pcode': 'object', 'md5sum': 'object'}
    },
    'person_info': {
        'columns': ['id', 'person_id', 'info_type_id', 'info', 'note'],
        'dtypes': {'id': 'Int64', 'person_id': 'Int64', 'info_type_id': 'Int64', 'info': 'object', 'note': 'object'}
    },
    'role_type': {
        'columns': ['id', 'role'],
        'dtypes': {'id': 'Int64', 'role': 'object'}
    },
    'title': {
        'columns': ['id', 'title', 'imdb_index', 'kind_id', 'production_year', 'imdb_id', 'phonetic_code', 'episode_of_id', 'season_nr', 'episode_nr', 'series_years', 'md5sum'],
        'dtypes': {'id': 'Int64', 'title': 'object', 'imdb_index': 'object', 'kind_id': 'Int64', 'production_year': 'Int64', 'imdb_id': 'Int64', 'phonetic_code': 'object', 'episode_of_id': 'Int64', 'season_nr': 'Int64', 'episode_nr': 'Int64', 'series_years': 'object', 'md5sum': 'object'}
    }
}

# Function to convert numpy types to native Python types
def convert_to_native_types(row):
    return tuple(int(x) if isinstance(x, (np.integer, pd.Int64Dtype)) else x for x in row)

# Function to populate a table from a CSV file
def populate_table_from_csv(cursor, table_name, file_path, columns, dtypes):
    try:
        df = pd.read_csv(file_path, names=columns, header=0, dtype=dtypes, na_values=['\\N', ''], keep_default_na=False, on_bad_lines='skip')  # Read all rows
        df.dropna(inplace=True)  # Drop rows with NA values
        print(f'Columns in {table_name}: {list(df.columns)}')
        columns_str = ', '.join(columns)
        values_str = ', '.join(['%s'] * len(columns))
        insert_query = f'INSERT INTO {table_name} ({columns_str}) VALUES ({values_str}) ON CONFLICT DO NOTHING'

        for row in df.itertuples(index=False, name=None):
            try:
                print(f'Inserting row into {table_name}: {row}')
                cursor.execute(insert_query, convert_to_native_types(row))
            except psycopg2.DataError as e:
                print(f"Data error in table {table_name} for row {row}: {e}")
                cursor.connection.rollback()  # Rollback only the problematic transaction
            except Exception as e:
                print(f"Error processing row {row} in table {table_name}: {e}")
                cursor.connection.rollback()  # Rollback only the problematic transaction
        cursor.connection.commit()
    except Exception as e:
        print(f"Error processing {table_name}: {e}")

# Establish connection to the database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

try:
    for table, schema in table_schemas.items():
        columns = schema['columns']
        dtypes = schema['dtypes']
        csv_file_path = os.path.join(csv_directory, f'{table}.csv')
        if os.path.exists(csv_file_path):
            print(f'Populating table {table} from {csv_file_path}')
            populate_table_from_csv(cur, table, csv_file_path, columns, dtypes)
            conn.commit()
            print(f'Table {table} populated successfully')
        else:
            print(f'CSV file for table {table} not found at {csv_file_path}')
except Exception as e:
    conn.rollback()
    print(f"An error occurred: {e}")
finally:
    cur.close()
    conn.close()
