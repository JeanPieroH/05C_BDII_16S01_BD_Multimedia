import os
import sys
import time
import random
from faker import Faker

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database import *

def _test_heapfile(n: int):
    fake = Faker()
    table_name = "heapfile_test"
    schema = [("codigo", "10s"), ("nombre", "20s"), ("precio", "f")]
    target_codigo = f"A{n // 2:05d}"

    create_table(table_name, schema, primary_key="codigo")

    print(f"== INSERTANDO {n} REGISTROS ==")
    for i in range(n):
        rec = Record(schema, [
            f"A{i:05d}",
            fake.first_name(),
            round(random.uniform(1.0, 100.0), 2)
        ])
        insert_record(table_name, rec)

    print(f"== BUSCANDO {target_codigo} ==")
    t1 = time.time()
    res = search_by_field(table_name, "codigo", target_codigo)
    t2 = time.time()
    print(f"{'ENCONTRADO' if res else 'NO ENCONTRADO'} en {t2 - t1:.6f} segundos")
    for r in res:
        print(r)

    print("== BORRANDO ==")
    delete_record(table_name, target_codigo)

    print("== BUSCANDO POST-BORRADO ==")
    res = search_by_field(table_name, "codigo", target_codigo)
    print(f"{'ENCONTRADO' if res else 'NO ENCONTRADO'}")

if __name__ == "__main__":
    _test_heapfile(1000)
