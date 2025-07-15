import os
import sys
import shutil

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database import (
    create_table,
    insert_record,
    search_by_field,
    drop_table,
)
from storage.Record import Record
from storage.Sound import Sound


def main():
    table_name = "songs_sound_test"
    schema = [
        ("id", "INT"),
        ("title", "VARCHAR(100)"),
        ("genre", "VARCHAR(50)"),
        ("audio", "SOUND"),
    ]
    primary_key = "id"

    # Clean up previous runs
    if os.path.exists(f"tables/{table_name}.dat"):
        drop_table(table_name)

    # 1. Create table
    create_table(table_name, schema, primary_key)
    print(f"Table '{table_name}' created.")

    # 2. Insert records
    records_to_insert = [
        (1, "Song A", "Pop", "000002.mp3"),
        (2, "Song B", "Rock", "000005.mp3"),
        (3, "Song C", "Jazz", "000010.mp3"),
    ]

    for r in records_to_insert:
        record = Record(schema, r)
        insert_record(table_name, record)
    print(f"{len(records_to_insert)} records inserted.")

    # 3. Verify insertion and sound path retrieval
    results = search_by_field(table_name, "id", 2)
    if results:
        retrieved_record = results[0]
        print(f"Retrieved record: {retrieved_record}")

        # Verification
        sound_path = retrieved_record.values[3]
        if sound_path == "000005.mp3":
            print("Test PASSED!")
        else:
            print("Test FAILED!")
    else:
        print("Test FAILED: Record not found.")

    # 5. Clean up
    drop_table(table_name)
    print(f"Table '{table_name}' dropped.")


if __name__ == "__main__":
    main()
