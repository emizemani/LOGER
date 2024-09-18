import os
import psycopg2

def load_data_to_postgres(data_directory, db_params):
    conn = None
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Iterate over all files in the data directory
        for filename in os.listdir(data_directory):
            if filename.endswith(".dat"):
                table_name = filename.split(".")[0]
                file_path = os.path.join(data_directory, filename)
                
                # Copy data from file to table
                with open(file_path, 'r', encoding='utf-8') as file:
                    cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH (FORMAT csv, DELIMITER '|')", file)
                
                print(f"Loaded data into table: {table_name}")
        
        # Commit the transaction
        conn.commit()
        print("All data loaded successfully.")
    
    except Exception as error:
        print(f"Error: {error}")
        if conn is not None:
            conn.rollback()
    
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

# Specify the data directory and database connection parameters
data_directory = '/home/emionatrip/tpcds_data_cleaned'
db_params = {
    'dbname': 'tpcds',
    'user': 'postgres',
    'password': 'thesis',
    'host': 'localhost',
    'port': '5433'
}

# Load data into PostgreSQL
load_data_to_postgres(data_directory, db_params)
