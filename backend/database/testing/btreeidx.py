import os
import sys
import time
import string
import random

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database import *
from storage.Record import Record

def crear_arbol_inicial():
    if os.path.exists("testtree.btree.idx"):
        os.remove("testtree.btree.idx")
    if os.path.exists("testtree.dat"):
        os.remove("testtree.dat")

    btree = BPlusTreeIndex(order=2, filename="testtree.dat", auxname="testtree.btree.idx", index_format='i')
    claves = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    for i, clave in enumerate(claves):
        btree.insert(IndexRecord('i', clave, 100 + i * 10))
    return btree

import os

def limpiar_archivos_btree(nombre_base: str):
    """
    Elimina los archivos del índice B+ Tree (.btree.idx y .btree.aux) si existen.
    
    Args:
        nombre_base: Ruta base (sin extensión) del índice, por ejemplo 'prueba.test'
    """
    for ext in [".btree.idx", ".btree.aux"]:
        path = f"{nombre_base}{ext}"
        if os.path.exists(path):
            os.remove(path)
            print(f"[DEBUG CLEAN] Archivo eliminado: {path}")
        else:
            print(f"[DEBUG CLEAN] No existe: {path}")

def test_eliminacion_caso_1_y_2():
    print("\n--- TEST CASOS 1 y 2 ---")

    limpiar_archivos_btree("prueba.test")

    btree = crear_arbol_inicial()

    print("\n--- Antes de eliminar ---")
    for k in [30, 40]:
        print(f"[DEBUG SEARCH] Buscando clave: {k}")
        print(f"Buscar {k}: {btree.search(k)}")

    offset_30 = btree.search(30)[0]
    btree.delete(30, offset_30)

    print("\n--- Después de eliminar 30 (caso 1) ---")
    print(f"Buscar 30: {btree.search(30)}")

    offset_40 = btree.search(40)[0]
    btree.delete(40, offset_40)

    print("\n--- Después de eliminar 40 (caso 2) ---")
    print(f"Buscar 40: {btree.search(40)}")

def test_eliminacion_caso_3_redistribucion():
    print("\n--- TEST CASO 3: Redistribución ---")
    btree = crear_arbol_inicial()
    for k in [30, 40, 50]:
        offset = btree.search(k)[0]
        btree.delete(k, offset)

    print("\n--- Después de eliminar 30, 40, 50 ---")
    for k in [30, 40, 50]:
        print(f"Buscar {k}: {btree.search(k)}")

def test_eliminacion_caso_4_fusion():
    print("\n--- TEST CASO 4: Fusión de hojas ---")
    from indexing.BPlusTreeIndex import BPlusTreeIndex, IndexRecord
    import os

    # Preparar entorno de prueba
    test_file = "fusion_test"
    aux_file = "fusion_test.codigo.btree.idx"

    # Eliminar archivos previos
    for f in [test_file, aux_file]:
        if os.path.exists(f):
            os.remove(f)

    # Crear árbol con orden 2 (max 2 claves por nodo)
    tree = BPlusTreeIndex(order=2, filename=test_file, auxname=aux_file, index_format='i')

    # Insertar claves que caen en distintas hojas y luego forzarán fusión
    claves = [10, 20, 30, 40, 50, 60]
    for i, clave in enumerate(claves):
        tree.insert(IndexRecord('i', clave, 100 + i * 10))

    print("\n[DEBUG] Árbol después de inserciones:")
    tree.scan_all()

    # Eliminar claves para dejar una hoja vacía y forzar fusión
    for clave in [30, 40, 50]:
        offset = 100 + claves.index(clave) * 10
        print(f"[DEBUG DELETE TEST] Eliminando clave: {clave}")
        tree.delete(clave, offset)

    print("\n[DEBUG] Árbol después de eliminar 30, 40, 50 (esperamos fusión de hojas):")
    tree.scan_all()

    # Confirmar que ya no existen
    for clave in [30, 40, 50]:
        result = tree.search(clave)
        print(f"[DEBUG BUSCAR {clave}] → {result}")

def test_delete_case5_fusion_internal_nodes():
    print("\n--- TEST CASO 5: Fusión en nodos internos ---")

    from indexing.BPlusTreeIndex import BPlusTreeIndex
    from indexing.IndexRecord import IndexRecord

    auxname = "test_case5.btree.idx"
    dataname = "test_case5.dat"

    # Eliminar archivos anteriores
    for f in [auxname, dataname]:
        if os.path.exists(f):
            os.remove(f)

    tree = BPlusTreeIndex(order=2, filename=dataname, auxname=auxname, index_format='i')

    # Insertar 15 claves para generar varios splits y nodos internos
    for i in range(15):
        key = (i + 1) * 10  # 10, 20, ..., 150
        tree.insert(IndexRecord('i', key, 100 + i * 10))

    print("\n[DEBUG] Árbol después de inserciones:")
    tree.scan_all()

    # Eliminar todas las claves menos 10 y 150 (borde)
    for i in range(1, 14):  # 20 hasta 140
        key = (i + 1) * 10
        print(f"[DEBUG DELETE TEST] Eliminando clave: {key}")
        tree.delete(key, 100 + i * 10)

    print("\n[DEBUG] Árbol después de eliminar múltiples claves (esperamos fusión de nodos internos):")
    tree.scan_all()

    # Validación
    for i in range(1, 14):
        key = (i + 1) * 10
        found = tree.search(key)
        print(f"[DEBUG BUSCAR {key}] → {found}")

if __name__ == "__main__":
    #test_eliminacion_caso_1_y_2()
    #test_eliminacion_caso_3_redistribucion()
    #test_eliminacion_caso_4_fusion()
    test_delete_case5_fusion_internal_nodes()
