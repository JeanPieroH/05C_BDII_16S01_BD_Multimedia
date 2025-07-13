import os
import sys
import time
import random
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database import *

def _test_insercion_heap_sin_pk(n: int):
    table_name = "SinPk"
    schema = [("id", "i"), ("nombre", "20s"), ("precio", "f")]

    create_table(table_name, schema, primary_key=None)

    print(f"== INSERTANDO {n} REGISTROS (sin PK) ==")
    t1 = time.time()
    for i in range(n):
        nombre = "P" + ''.join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=5))
        precio = round(random.uniform(1.0, 100.0), 2)
        rec = Record(schema, [i + 1, nombre, precio])
        insert_record_free(table_name, rec)
    t2 = time.time()

    print(f"== INSERCIÓN COMPLETA en {t2 - t1:.6f} segundos ==")

if __name__ == "__main__":
    _test_insercion_heap_sin_pk(10)