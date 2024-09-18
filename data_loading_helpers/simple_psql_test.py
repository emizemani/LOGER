import sqlalchemy
from sqlalchemy.sql import text

# Define your database URI
db_uri = 'postgresql://postgres:thesis@127.0.0.1/imdb'

# Create an engine and bind metadata
engine = sqlalchemy.create_engine(db_uri)
metadata = sqlalchemy.MetaData()
metadata.bind = engine

# Define a simple test table
test_table = sqlalchemy.Table(
    'test_table', metadata,
    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column('name', sqlalchemy.String)
)

# Create the table, bind the engine
metadata.create_all(engine)
print("Table created successfully.")

# Insert a test row with explicit transaction handling
with engine.begin() as conn:
    result = conn.execute(test_table.insert(), [{'id': 1, 'name': 'Test Name'}])
    print(f"Inserted row: {result.rowcount} rows affected.")

# Verify the insertion using raw SQL
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM test_table")).fetchall()
    print(f"Query result: {result}")  # Should print [(1, 'Test Name')]
