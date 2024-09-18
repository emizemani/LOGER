import os
import torch
from tqdm import tqdm
from lib import filepath as fp
from core.sql import Sql
from lib.timer import timer
from core import database
from lib.cache import HashCache

# Function to load SQL files from a directory
def _load(directory, verbose=False):
    L = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            L.append(os.path.join(root, file))
    res = []
    if verbose:
        L = tqdm(L, desc='Loading files')
    for file in L:
        with open(file, 'r') as f:
            data = ' '.join(f.readlines())
            res.append((data, file))
    return res

# Function to parse and load SQL files into Sql objects
def load_sqls(config, directory, device=torch.device('cpu'), verbose=False):
    _timer = timer()
    res = []
    gen = _load(directory, verbose=verbose)
    if verbose:
        gen = tqdm(gen, desc='Parsing')
    for sql, filename in gen:
        fname = fp.path_split(filename)[-1]
        with _timer:
            sql_obj = Sql(sql, config.feature_length, filename=fname)
        sql_obj.to(device)
        res.append(sql_obj)
    return res

# Function to get cost of SQL query
def get_sql_cost(sql):
    try:
        cost_value = database.latency(str(sql), cache=False)
        return cost_value
    except Exception as e:
        print(f"Error getting cost for SQL {sql.filename}: {e}")
        return None

# Main function to test loading and processing
def main():
    dataset_dir = '/home/emionatrip/Desktop/thesis/LOGER/dataset/train'  # Change this to your dataset directory
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    # Set up the database configuration
    dbname = 'imdb'
    user = 'postgres'
    password = 'thesis'
    port = 5433
    try:
        database.setup(dbname=dbname, user=user, password=password, port=port, cache=False)
        config = database.config
        print(f"Database configuration set for {dbname} at port {port}")
    except Exception as e:
        print(f"Error setting up database configuration: {e}")
        return

    print(f"Loading SQL queries from {dataset_dir}")
    sql_queries = load_sqls(config, dataset_dir, device=device, verbose=True)

    if not sql_queries:
        print("No SQL queries loaded.")
        return

    print(f"Loaded {len(sql_queries)} SQL queries.")

    cache_manager = HashCache()

    for sql in sql_queries:
        print(f"SQL filename: {sql.filename}")
        cost_value = get_sql_cost(sql)
        if cost_value is not None:
            print(f"Cost for SQL {sql.filename}: {cost_value}")
        else:
            print(f"Failed to get cost for SQL {sql.filename}")

if __name__ == "__main__":
    main()
