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
    build_acoustic_index,
    knn_search,
    knn_search_index,
)
from storage.Record import Record


def main():
    table_name = "songs_knn_index"
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
    if os.path.exists(f"backend/database/tables/acoustic_index.dat"):
        os.remove(f"backend/database/tables/acoustic_index.dat")
    if os.path.exists(f"backend/database/tables/acoustic_index.schema.json"):
        os.remove(f"backend/database/tables/acoustic_index.schema.json")
    if os.path.exists(f"backend/database/tables/acoustic_index_norms.dat"):
        os.remove(f"backend/database/tables/acoustic_index_norms.dat")
    if os.path.exists(f"backend/database/tables/acoustic_index_norms.schema.json"):
        os.remove(f"backend/database/tables/acoustic_index_norms.schema.json")

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
    print("Acoustic model built.")

    # 4. Build acoustic index
    build_acoustic_index(table_name, field_name)
    print("Acoustic index built.")

    # 5. Perform k-NN search
    query_audio_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sounds', '000207.mp3'))

    print("\n--- Sequential Search ---")
    results_seq = knn_search(table_name, field_name, query_audio_path, k)
    print(f"Top {k} most similar songs to '{os.path.basename(query_audio_path)}':")
    for record, similarity in results_seq:
        print(f"  - Record: {record}, Similarity: {similarity:.4f}")

    print("\n--- Index Search ---")
    results_idx = knn_search_index(table_name, field_name, query_audio_path, k)
    print(f"Top {k} most similar songs to '{os.path.basename(query_audio_path)}':")
    for record, similarity in results_idx:
        print(f"  - Record: {record}, Similarity: {similarity:.4f}")

    # 6. Compare results
    results_seq_gt_0 = {r[0].values[0] for r in results_seq if r[1] > 0}
    results_idx_gt_0 = {r[0].values[0] for r in results_idx if r[1] > 0}

    if results_seq_gt_0 == results_idx_gt_0 and len(results_seq) == k and len(results_idx) == k:
        print("\nTest PASSED! Search results are consistent.")
    else:
        print("\nTest FAILED! Search results are inconsistent.")
        print(f"Sequential (>0): {sorted(list(results_seq_gt_0))}")
        print(f"Index (>0):      {sorted(list(results_idx_gt_0))}")
        print(f"Sequential len: {len(results_seq)}")
        print(f"Index len:      {len(results_idx)}")

    # 7. Clean up
    drop_table(table_name)
    if os.path.exists(f"backend/database/tables/{table_name}.{field_name}.codebook.pkl"):
        os.remove(f"backend/database/tables/{table_name}.{field_name}.codebook.pkl")
    if os.path.exists(f"backend/database/tables/{table_name}.{field_name}.histogram.dat"):
        os.remove(f"backend/database/tables/{table_name}.{field_name}.histogram.dat")
    if os.path.exists(f"backend/database/tables/acoustic_index.dat"):
        os.remove(f"backend/database/tables/acoustic_index.dat")
    if os.path.exists(f"backend/database/tables/acoustic_index.schema.json"):
        os.remove(f"backend/database/tables/acoustic_index.schema.json")
    if os.path.exists(f"backend/database/tables/acoustic_index_norms.dat"):
        os.remove(f"backend/database/tables/acoustic_index_norms.dat")
    if os.path.exists(f"backend/database/tables/acoustic_index_norms.schema.json"):
        os.remove(f"backend/database/tables/acoustic_index_norms.schema.json")
    print(f"Table '{table_name}' dropped and associated files removed.")


if __name__ == "__main__":
    main()
