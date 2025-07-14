# test_spimi_internal_structure.py

import os
import sys
import json
import pandas as pd
from collections import defaultdict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database import *
from storage.Record import Record
from storage.HeapFile import HeapFile
from indexing.ExtendibleHashIndex import ExtendibleHashIndex

def _test_spimi_verification():
    # 1. Crear tabla de prueba pequeña
    table_name = "test_spimi"
    schema = [
        ("id", "i"),
        ("title", "text"),
        ("content", "text")
    ]
    
    # Datos de prueba (fáciles de verificar)
    test_data = [
        [1, "hello world", "this is a test about python programming"],
        [2, "goodbye world", "python is great but java exists"],
        [3, "hello hello again", "another test with python and spimi"]
    ]
    
    print(f"\n== CREANDO TABLA '{table_name}' ==")
    create_table(table_name, schema, primary_key="id")
    
    print(f"\n== INSERTANDO {len(test_data)} REGISTROS ==")
    for row in test_data:
        rec = Record(schema, row)
        insert_record(table_name, rec)
    
    # 2. Inspeccionar tabla original
    print("\n=== TABLA ORIGINAL ===")
    print_table(table_name)
    
    # 3. Construir índice SPIMI
    print("\n== CONSTRUYENDO ÍNDICE SPIMI ==")
    build_spimi_index(table_name)
    
    # 4. Verificar estructuras generadas
    print("\n=== ÍNDICE INVERTIDO ===")
    print_table("inverted_index")  # Asumiendo que es el nombre por defecto
    
    print("\n=== NORMAS DE DOCUMENTOS ===")
    print_table("inverted_index_norms")
    
    # 5. Verificar términos específicos
    print("\n=== TÉRMINOS CLAVE ===")
    terms_to_check = ["hello", "python", "test"]
    for term in terms_to_check:
        print(f"\nPostings para '{term}':")
        results = search_hash_idx("inverted_index", "term", term)
        for r in results:
            print(r)
            

if __name__ == "__main__":
    _test_spimi_verification()