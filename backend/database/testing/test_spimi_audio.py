import os
import sys
import shutil

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database import (
    create_table,
    insert_record,
    drop_table,
    build_acoustic_model,
    search_audio_knn,
)
from storage.Record import Record
from storage.Sound import Sound
from indexing.SpimiAudio import SpimiAudio
from storage.HistogramFile import HistogramFile
from storage.HeapFile import HeapFile
from database import _table_path
import pickle


def main():
    table_name = "songs_spimi_knn"
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
    index_name = "spimi_audio_index"

    # Clean up previous runs
    if os.path.exists(f"backend/database/tables/{table_name}.dat"):
        drop_table(table_name)
    if os.path.exists(f"data/indexes/{table_name}/{index_name}"):
        shutil.rmtree(f"data/indexes/{table_name}/{index_name}")


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
    print("Acoustic model built.")

    # 4. Build SpimiAudio index
    heap_file = HeapFile(_table_path(table_name))
    histogram_file = HistogramFile(_table_path(table_name), field_name)
    histograms = []
    for _, record in heap_file.get_all_records():
        pk_val = record.values[heap_file.schema.index((primary_key, "INT"))]
        _, hist_offset = record.values[heap_file.schema.index((field_name, "SOUND"))]
        hist = histogram_file.read(hist_offset)
        histograms.append((pk_val, dict(hist)))

    spimi_audio = SpimiAudio(table_name, index_name, _table_path)
    spimi_audio.build(histograms)
    spimi_audio._calculate_tf_idf(len(records_to_insert))
    print("SpimiAudio index built.")

    # 5. Perform k-NN search
    query_audio_path = "backend/database/sounds/000207.mp3"
    results = search_audio_knn(table_name, field_name, index_name, query_audio_path, k)

    # 6. Print results
    print(f"\nTop {k} most similar songs to '{query_audio_path}':")
    for record, score in results:
        print(f"  - Record: {record}, Similarity: {score:.4f}")

    if len(results) == k:
        print("\nTest PASSED!")
    else:
        print(f"\nTest FAILED: Expected {k} results, but got {len(results)}.")

    # 7. Clean up
    drop_table(table_name)
    if os.path.exists(f"data/indexes/{table_name}/{index_name}"):
        shutil.rmtree(f"data/indexes/{table_name}/{index_name}")
    print(f"Table '{table_name}' dropped and associated files removed.")


if __name__ == "__main__":
    main()
