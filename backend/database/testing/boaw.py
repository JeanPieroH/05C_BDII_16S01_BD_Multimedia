import os
import sys
import shutil

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database import (
    create_table,
    insert_record,
    drop_table,
    build_acoustic_model,
    search_by_field,
)
from storage.Record import Record
from storage.Sound import Sound


def main():
    table_name = "songs"
    field_name = "audio"
    schema = [
        ("id", "INT"),
        ("title", "VARCHAR(100)"),
        ("genre", "VARCHAR(50)"),
        (field_name, "SOUND"),
    ]
    primary_key = "id"
    num_clusters = 5

    # Clean up previous runs
    if os.path.exists(f"backend/database/tables/{table_name}.dat"):
        drop_table(table_name)
    if os.path.exists(f"backend/database/tables/{table_name}.{field_name}.codebook.pkl"):
        os.remove(f"backend/database/tables/{table_name}.{field_name}.codebook.pkl")

    # 1. Create table
    create_table(table_name, schema, primary_key)
    print(f"Table '{table_name}' created.")

    # 2. Insert records
    records_to_insert = [
        (1, "Song A", "Pop", "sounds/000002.mp3"),
        (2, "Song B", "Rock", "sounds/000005.mp3"),
        (3, "Song C", "Jazz", "sounds/000010.mp3"),
        (4, "Song D", "Pop", "sounds/000140.mp3"),
        (5, "Song E", "Rock", "sounds/000141.mp3"),
        (6, "Song F", "Jazz", "sounds/000148.mp3"),
    ]

    for r in records_to_insert:
        record = Record(schema, r)
        insert_record(table_name, record)
    print(f"{len(records_to_insert)} records inserted.")

    # 3. Build acoustic model
    build_acoustic_model(table_name, field_name, num_clusters)

    # 4. Verify histogram
    sound_handler = Sound(f"backend/database/tables/{table_name}_hist", field_name)
    # Since we are not storing the histogram in the original table, we can't search by it.
    # We will just check if the histogram file is created.
    if os.path.exists(f"backend/database/tables/{table_name}_hist.{field_name}.dat"):
        print("Test PASSED!")
    else:
        print("Test FAILED!")

    # 5. Clean up
    drop_table(table_name)
    if os.path.exists(f"backend/database/tables/{table_name}.{field_name}.codebook.pkl"):
        os.remove(f"backend/database/tables/{table_name}.{field_name}.codebook.pkl")
    print(f"Table '{table_name}' dropped and codebook removed.")


if __name__ == "__main__":
    main()
