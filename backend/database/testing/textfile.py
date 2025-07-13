import os
import sys
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database import *
import random
from faker import Faker

def _test_text_table(n: int):
    fake = Faker()
    table_name = "text_test"
    schema = [("id", "i"), ("titulo", "text"), ("contenido", "text")]
    pk = "id"

    def short_text(text, max_len=70):
        """Corta texto a un largo aleatorio ≤ max_len (entre 20 y 70)."""
        limit = random.randint(15, max_len)
        return text[:limit]

    print(f"\n== CREANDO TABLA {table_name} ==")
    create_table(table_name, schema, primary_key=pk)

    print(f"\n== INSERTANDO {n} REGISTROS CON CAMPOS TEXT CORTOS ==")
    for i in range(1, n + 1):
        titulo = short_text(fake.sentence(nb_words=8), 30)
        contenido = short_text(fake.paragraph(nb_sentences=3), 60)
        rec = Record(schema, [i, titulo, contenido])
        insert_record(table_name, rec)

    print(f"\n== CONTENIDO COMPLETO DE LA TABLA {table_name} ==")
    print_table(table_name)

    target_id = n // 2
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
    _test_text_table(10)