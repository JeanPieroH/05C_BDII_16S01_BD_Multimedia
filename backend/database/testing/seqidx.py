import os
import sys
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database import *
import random
from faker import Faker

def _test_seqidx(n: int):
    fake = Faker()

    table_name = "seqidx_test"
    schema = [("codigo", "10s"), ("nombre", "20s"), ("precio", "f")]
    pk = "codigo"

    create_table(table_name, schema, pk)
    create_seq_idx(table_name, "codigo")

    target_index = n // 2
    target_codigo = f"A{target_index:05d}"
    range_inicio = f"A{(n // 3):05d}"
    range_final = f"A{(2 * n // 3):05d}"

    print(f"\n== INSERTANDO {n} REGISTROS ==")
    for i in range(n):
        codigo = f"A{i:05d}"
        nombre = fake.first_name()
        precio = round(random.uniform(1.0, 100.0), 2)
        rec = Record(schema, [codigo, nombre, precio])
        insert_record(table_name, rec)

    print(f"\n== BUSCANDO {target_codigo} ==")
    t1 = time.time()
    r1 = search_by_field(table_name, "codigo", target_codigo)
    t2 = time.time()
    print(f"Sin índice: {'ENCONTRADO' if r1 else 'NO ENCONTRADO'} en {t2 - t1:.6f} s")

    t3 = time.time()
    r2 = search_seq_idx(table_name, "codigo", target_codigo)
    t4 = time.time()
    print(f"Con índice secuencial: {'ENCONTRADO' if r2 else 'NO ENCONTRADO'} en {t4 - t3:.6f} s")

    print(f"\n== BUSQUEDA POR RANGO [{range_inicio} - {range_final}] ==")
    t5 = time.time()
    resultados = search_seq_idx_range(table_name, "codigo", range_inicio, range_final)
    t6 = time.time()
    print(f"{len(resultados)} registros encontrados en {t6 - t5:.6f} s")
    for r in resultados[:5]:
        print("Ejemplo:", r)

    print("\n== BORRANDO ==")
    delete_record(table_name, target_codigo)

    print(f"\n== BUSCANDO {target_codigo} POST-BORRADO ==")
    print("Sin índice:")
    r3 = search_by_field(table_name, "codigo", target_codigo)
    print(f"{'ENCONTRADO' if r3 else 'NO ENCONTRADO'}")

    print("Con índice secuencial:")
    r4 = search_seq_idx(table_name, "codigo", target_codigo)
    print(f"{'ENCONTRADO' if r4 else 'NO ENCONTRADO'}")

if __name__ == "__main__":
    _test_seqidx(1000)