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
    knn_search,
)
from storage.Record import Record
from storage.Sound import Sound


def main():
    table_name = "songs_knn"
    field_name = "audio"
    schema = [
        ("id", "INT"),
        ("title", "VARCHAR(100)"),
        ("genre", "VARCHAR(50)"),
        (field_name, "SOUND"),
    ]
    primary_key = "id"
    num_clusters = 3
    k = 3

    # Clean up previous runs
    if os.path.exists(f"backend/database/tables/{table_name}.dat"):
        drop_table(table_name)
    if os.path.exists(f"backend/database/tables/{table_name}.{field_name}.codebook.pkl"):
        os.remove(f"backend/database/tables/{table_name}.{field_name}.codebook.pkl")
    if os.path.exists(f"backend/database/tables/{table_name}.{field_name}.histogram.dat"):
        os.remove(f"backend/database/tables/{table_name}.{field_name}.histogram.dat")

    # 1. Create table
    create_table(table_name, schema, primary_key)
    print(f"Table '{table_name}' created.")

    # 2. Insert records
    records_to_insert = [
        (1, "Song A", "Pop", "000002.mp3"),
        (2, "Song B", "Rock", "000005.mp3"),
        (3, "Song C", "Jazz", "000010.mp3"),
        (4, "Song D", "Pop", "000140.mp3"),
        (5, "Song E", "Rock", "000141.mp3"),
        (6, "Song F", "Jazz", "000148.mp3"),
    ]

    for r in records_to_insert:
        record = Record(schema, r)
        insert_record(table_name, record)
    print(f"{len(records_to_insert)} records inserted.")

    # 3. Build acoustic model
    build_acoustic_model(table_name, field_name, num_clusters)

    # 4. Perform k-NN search
    query_audio_path = "backend/database/sounds/000207.mp3"
    results = knn_search(table_name, field_name, query_audio_path, k)

    # 5. Print results
    print(f"\nTop {k} most similar songs to '{query_audio_path}':")
    for similarity, record in results:
        print(f"  - Record: {record}, Similarity: {similarity:.4f}")

    if len(results) == k:
        print("\nTest PASSED!")
    else:
        print(f"\nTest FAILED: Expected {k} results, but got {len(results)}.")

    # 6. Clean up
    drop_table(table_name)
    #if os.path.exists(f"backend/database/tables/{table_name}.{field_name}.codebook.pkl"):
    #    os.remove(f"backend/database/tables/{table_name}.{field_name}.codebook.pkl")
    if os.path.exists(f"backend/database/tables/{table_name}.{field_name}.histogram.dat"):
        os.remove(f"backend/database/tables/{table_name}.{field_name}.histogram.dat")
    print(f"Table '{table_name}' dropped and associated files removed.")


if __name__ == "__main__":
    main()
