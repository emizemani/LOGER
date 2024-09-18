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
csv_directory = '/home/emionatrip/Desktop/thesis/LOGER/imdb_data_job/'

# Table schema for person_info
person_info_schema = {
    'columns': ['id', 'person_id', 'info_type_id', 'info', 'note'],
    'dtypes': {'id': 'Int64', 'person_id': 'Int64', 'info_type_id': 'Int64', 'info': 'object', 'note': 'object'}
}

# Function to convert numpy types to native Python types and handle None
def convert_to_native_types(row):
    return tuple("" if pd.isna(x) or isinstance(x, str) and pd.isnull(x) else int(x) if isinstance(x, (np.integer, pd.Int64Dtype)) else x for x in row)

# Function to populate a table from a CSV file with detailed logging
def populate_table_from_csv(cursor, table_name, file_path, columns, dtypes, batch_size=1000):
    try:
        df = pd.read_csv(file_path, names=columns, header=0, dtype=dtypes, na_values=['\\N', ''], keep_default_na=False, on_bad_lines='skip')
        print(f'Columns in {table_name}: {list(df.columns)}')
        columns_str = ', '.join(columns)
        values_str = ', '.join(['%s'] * len(columns))
        insert_query = f'INSERT INTO {table_name} ({columns_str}) VALUES ({values_str}) ON CONFLICT DO NOTHING'

        batch = []
        for row in df.itertuples(index=False, name=None):
            try:
                native_row = convert_to_native_types(row)
                # Replace unparseable values with empty strings
                native_row = tuple("" if isinstance(x, Exception) else x for x in native_row)
                batch.append(native_row)
                if len(batch) >= batch_size:
                    print(f'Inserting batch into {table_name}: {batch[:5]}...')  # Log only the first 5 rows of the batch
                    cursor.executemany(insert_query, batch)
                    cursor.connection.commit()
                    batch = []
            except Exception as row_error:
                print(f"Error processing row {row} in table {table_name}: {row_error}")
        if batch:
            print(f'Inserting final batch into {table_name}: {batch[:5]}...')  # Log only the first 5 rows of the final batch
            cursor.executemany(insert_query, batch)
            cursor.connection.commit()
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file {file_path} for table {table_name}: {e}")
    except Exception as e:
        print(f"Error processing {table_name}: {e}")

# Establish connection to the database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

try:
    table_name = 'person_info'
    columns = person_info_schema['columns']
    dtypes = person_info_schema['dtypes']
    csv_file_path = os.path.join(csv_directory, f'{table_name}.csv')
    if os.path.exists(csv_file_path):
        print(f'Populating table {table_name} from {csv_file_path}')
        populate_table_from_csv(cur, table_name, csv_file_path, columns, dtypes)
        conn.commit()
        print(f'Table {table_name} populated successfully')
    else:
        print(f'CSV file for table {table_name} not found at {csv_file_path}')
except Exception as e:
    conn.rollback()
    print(f"An error occurred: {e}")
finally:
    cur.close()
    conn.close()
