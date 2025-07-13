import os
import sys
import time
import string
import random

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database import *
from storage.Record import Record

import time

def test_create_student_table_with_data(n=10):
    """
    Crea una tabla 'alumnos' con índices sobre campos clave
    y genera n registros aleatorios para pruebas. Guarda los tiempos en resultados_test.txt.
    """
    table_name = "alumnos"
    schema = [
        ("codigo", "i"),           # Clave primaria
        ("nombre", "20s"),         # Nombre
        ("pension", "f"),          # Pensión mensual
        ("ciclo", "i"),            # Ciclo actual
        ("especialidad", "15s"),   # Carrera
    ]
    primary_key = "codigo"

    # Preparar tabla e índices
    create_table_with_btree_pk(table_name, schema, primary_key)
    create_seq_idx(table_name, "pension")
    create_hash_idx(table_name, "nombre")
    create_btree_idx(table_name, "ciclo")

    nombres = ["Ana", "Luis", "Carlos", "María", "Elena", "Jorge", "Lucía", "Pedro", "Sofía", "Diego"]
    carreras = ["Sistemas", "Industrial", "Civil", "Ambiental", "Mecánica", "Contabilidad"]

    codigos_usados = set()
    start_time = time.time()

    for _ in range(n):
        while True:
            codigo = random.randint(10000, 99999)
            if codigo not in codigos_usados:
                codigos_usados.add(codigo)
                break

        nombre = random.choice(nombres)
        pension = round(random.uniform(300.0, 1500.0), 2)
        ciclo = random.randint(1, 10)
        especialidad = random.choice(carreras)

        record = Record(schema, [codigo, nombre, pension, ciclo, especialidad])
        insert_record_btree_pk(table_name, record)

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"✅ Insertados {n} alumnos en la tabla '{table_name}'.")

    # Guardar en archivo
    with open("resultados_test.txt", "a", encoding="utf-8") as f:
        f.write(f"🔢 Insertados {n} registros con índices (btree + hash + seq) en {elapsed:.4f} segundos.\n\n")


def test_search_comparison(n):
    table_name = "alumnos"

    test_nombre = "Luis"
    test_pension_min, test_pension_max = 500.0, 1000.0
    test_ciclo = 5

    resultados = []
    resultados.append(f"🔍 Comparando búsquedas con {n} registros en la tabla '{table_name}':\n")

    # Hash vs Secuencial
    t0 = time.time()
    result_hash = search_hash_idx(table_name, "nombre", test_nombre)
    t1 = time.time()
    result_seq = search_by_field(table_name, "nombre", test_nombre)
    t2 = time.time()
    resultados.append(f"🔸 Nombre = '{test_nombre}'")
    resultados.append(f"Hash Index     → {len(result_hash)} resultados en {t1 - t0:.6f} s")
    resultados.append(f"Búsqueda Lineal→ {len(result_seq)} resultados en {t2 - t1:.6f} s\n")

    # Pensión: secuencial sin índice vs con índice
    t0 = time.time()
    result_heap = [r for r in search_by_field(table_name, "pension", None) if test_pension_min <= r.values[2] <= test_pension_max]
    t1 = time.time()
    result_seq_idx = search_seq_idx_range(table_name, "pension", test_pension_min, test_pension_max)
    t2 = time.time()
    resultados.append(f"🔸 Pensión en rango [{test_pension_min}, {test_pension_max}]")
    resultados.append(f"Heap Scan      → {len(result_heap)} resultados en {t1 - t0:.6f} s")
    resultados.append(f"SequentialIdx  → {len(result_seq_idx)} resultados en {t2 - t1:.6f} s\n")

    # B+Tree vs Secuencial
    t0 = time.time()
    result_btree = search_btree_idx(table_name, "ciclo", test_ciclo)
    t1 = time.time()
    result_seq_ciclo = search_by_field(table_name, "ciclo", test_ciclo)
    t2 = time.time()
    resultados.append(f"🔸 Ciclo = {test_ciclo}")
    resultados.append(f"B+Tree Index   → {len(result_btree)} resultados en {t1 - t0:.6f} s")
    resultados.append(f"Búsqueda Lineal→ {len(result_seq_ciclo)} resultados en {t2 - t1:.6f} s\n")

    print("\n".join(resultados))

    with open("resultados_test.txt", "a", encoding="utf-8") as f:
        f.write("\n".join(resultados))
        f.write("\n" + "="*60 + "\n\n")


if __name__ == "__main__":
    n = 1000
    test_create_student_table_with_data(n)
    test_search_comparison(n)