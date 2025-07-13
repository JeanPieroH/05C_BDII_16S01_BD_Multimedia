import os
import sys
import time
import random
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database import *

def _test_text_table_from_csv(path: str):
    table_name = "text_news"
    schema = [("id", "i"), ("title", "text"), ("content", "text"), ("year", "i"), ("author", "30s")]
    pk = "id"

    print(f"\n== LEYENDO CSV DESDE {path} ==")
    df = pd.read_csv(path)

    print(f"\n== CREANDO TABLA {table_name} ==")
    create_table(table_name, schema, primary_key="id")

    print(f"\n== INSERTANDO {len(df)} REGISTROS ==")
    for _, row in df.iterrows():
        values = [int(row["id"]), row["title"], row["content"], int(row["year"]), row["author"]]
        rec = Record(schema, values)
        insert_record(table_name, rec)

    print(f"\n== CONTENIDO COMPLETO DE LA TABLA {table_name} ==")
    print_table(table_name)

    target_id = df["id"].iloc[len(df) // 2]
    print(f"\n== BUSCANDO POR ID {target_id} ==")
    result = search_by_field(table_name, "id", target_id)
    if result:
        print("ENCONTRADO:", result[0])
    else:
        print("NO ENCONTRADO")

    print(f"\n== BORRANDO ID {target_id} ==")
    delete_record(table_name, target_id)

    print(f"\n== BUSCANDO NUEVAMENTE POR ID {target_id} ==")
    result = search_by_field(table_name, "id", target_id)
    if result:
        print("TODAVÍA EXISTE:", result[0])
    else:
        print("REGISTRO ELIMINADO")

    print(f"\n== IMPRESIÓN FINAL DE LA TABLA {table_name} ==")
    print_table(table_name)

if __name__ == "__main__":
    current_dir = os.path.dirname(__file__)
    csv_path = os.path.join(current_dir, "news.csv")
    _test_text_table_from_csv(csv_path)

