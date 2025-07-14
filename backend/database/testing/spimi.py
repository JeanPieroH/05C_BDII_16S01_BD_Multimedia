# test_spimi_news.py

import os
import sys
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database import *
from storage.Record import Record

def _test_spimi_from_news_csv(path: str):
    table_name = "news_text"
    schema = [
        ("id", "i"),
        ("title", "text"),
        ("content", "text"),
        ("year", "i"),
        ("author", "30s")
    ]

    print(f"== LEYENDO CSV DESDE {path} ==")
    df = pd.read_csv(path)

    print(f"\n== CREANDO TABLA '{table_name}' ==")
    create_table(table_name, schema, primary_key="id")

    print(f"\n== INSERTANDO {len(df)} REGISTROS ==")
    for _, row in df.iterrows():
        values = [
            int(row["id"]),
            str(row["title"]),
            str(row["content"]),
            int(row["year"]),
            str(row["author"])
        ]
        rec = Record(schema, values)
        insert_record(table_name, rec)

    print(f"\n== CONSTRUYENDO ÍNDICE SPIMI PARA '{table_name}' ==")
    build_spimi_index(table_name)

    query = "trump"
    print(f"\n== CONSULTA: '{query}' ==")
    results = search_text(table_name, query, k=5)

    print("\nTop-5 documentos similares:")
    for rec, score in results:
        print()
        print(rec, score)

if __name__ == "__main__":
    current_dir = os.path.dirname(__file__)
    csv_path = os.path.join(current_dir, "news.csv") 
    _test_spimi_from_news_csv(csv_path)
