import psycopg2

# Database connection parameters
db_params = {
    'dbname': 'imdbload',
    'user': 'postgres',
    'password': 'thesis',  # Replace with your actual password
    'host': 'localhost',
    'port': '5433'
}

# Path to the SQL file
sql_file_path = '/home/emionatrip/Desktop/thesis/LOGER/fkindexes.sql'

# Read the SQL file
with open(sql_file_path, 'r') as file:
    sql_commands = file.read()

# Establish connection to the database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Execute the SQL commands
try:
    cur.execute(sql_commands)
    conn.commit()
    print("Tables created successfully")
except Exception as e:
    conn.rollback()
    print(f"An error occurred: {e}")
finally:
    cur.close()
    conn.close()
