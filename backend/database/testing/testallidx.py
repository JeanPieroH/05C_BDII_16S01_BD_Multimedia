import os
import sys
import time
import random
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database import *

def create_test_table(table_name: str, n: int) -> None:
    schema = [
        ("id", "i"),    # Para Sequential Index
        ("name", "20s"),    # Para Hash Index
        ("value", "f"),     # Para B-Tree Index
        ("coord", "2f")     # Para R-Tree Index
    ]

    create_table(table_name, schema, "id")
    create_seq_idx(table_name, "id")
    create_hash_idx(table_name, "name")
    create_btree_idx(table_name, "value")
    create_rtree_idx(table_name, "coord")

    for i in range(1, n + 1):
        record = Record(schema, [i, f"name_{i}", random.uniform(0.0, 100.0), (random.uniform(0.0, 100.0), random.uniform(0.0, 100.0))])
        insert_record(table_name, record)

def test_search(table_name: str) -> None:
    print (f"\nTesting search on field 'id' in table {table_name}\n")
    start_time = time.time()
    record: list = search_by_field(table_name, "id", 50)
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Search in field 'id' without index took {total_time:.6f} seconds, found {len(record)} records.")
    start_time = time.time()
    record: list = search_seq_idx(table_name, "id", 50)
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Search in field 'id' with sequential index took {total_time:.6f} seconds, found {len(record)} records.\n")

    print(f"Testing search on field 'name' in table {table_name}\n")
    start_time = time.time()
    record: list = search_by_field(table_name, "name", "name_50")
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Search in field 'name' without index took {total_time:.6f} seconds, found {len(record)} records.")
    start_time = time.time()
    record: list = search_hash_idx(table_name, "name", "name_50")
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Search in field 'name' with hash index took {total_time:.6f} seconds, found {len(record)} records.\n")

    print(f"Testing search on field 'value' in table {table_name}\n")
    start_time = time.time()
    record: list = search_by_field(table_name, "value", 50.0)
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Search in field 'value' without index took {total_time:.6f} seconds, found {len(record)} records.")
    start_time = time.time()
    record: list = search_btree_idx(table_name, "value", 50.0)
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Search in field 'value' with B-Tree index took {total_time:.6f} seconds, found {len(record)} records.\n")

    print(f"Testing search on field 'coord' in table {table_name}\n")
    start_time = time.time()
    record: list = search_by_field(table_name, "coord", (50.0, 50.0))
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Search in field 'coord' without index took {total_time:.6f} seconds, found {len(record)} records.")
    start_time = time.time()
    record: list = search_rtree_record(table_name, "coord", (50.0, 50.0))
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Search in field 'coord' with R-Tree index took {total_time:.6f} seconds, found {len(record)} records.\n")

def drop_test_table(table_name: str) -> None:
    drop_table(table_name)

if __name__ == "__main__":
    table_name = "test_all_idx"
    create_test_table(table_name, 10000)
    #test_search(table_name)
    drop_test_table(table_name)