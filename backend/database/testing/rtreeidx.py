import os
import sys
import time
import random
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database import *

def _test_create_rtreeidx(table_name: str, n: int):
    schema = [("id", "i"), ("coord", "2f")]
    create_table(table_name, schema, primary_key="id")
    create_rtree_idx(table_name, "coord")
    print(f"Test RTree Index with {n} records")

    for i in range(1, n + 1):
        record = Record(schema, [int(i), (random.uniform(-100, 100), random.uniform(-100, 100))])
        insert_record(table_name, record)

    print_rtree_idx(table_name, "coord")

    records = search_rtree_knn(table_name, "coord", (0, 0), 10)
    print(f"Found {len(records)} records near (0, 0):")
    for rec in records:
        print(rec)
    
    drop_table(table_name)


if __name__ == "__main__":
    _test_create_rtreeidx("RTreeTest", 100)