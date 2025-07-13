import os
import pickle
import math
from collections import defaultdict
import heapq
import sys
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from .utils_spimi import preprocess
from storage.HeapFile import HeapFile
from .ExtendibleHashIndex import ExtendibleHashIndex
from storage.Record import Record


class SPIMIIndexer:
    def __init__(self, block_dir="index_blocks", index_table_name="inverted_index"):
        self.block_dir = block_dir
        self.index_table_name = index_table_name
        os.makedirs(self.block_dir, exist_ok=True)

    def build_index(self, table_name: str):
        self.doc_count = 0
        self._process_documents(table_name)
        full_index, document_norms = self._external_merge_blocks_with_tfidf()
        self._save_index_to_table(full_index, document_norms)
        self._clean_blocks()

    def _process_documents(self, table_name: str):
        heapfile: HeapFile = HeapFile(table_name)
        term_dict = defaultdict(lambda: defaultdict(int))
        block_number = 0
        memory_limit = 100 * 1024 * 1024  # 100 MB

        for doc_id, text in heapfile.iterate_text_documents():
            self.doc_count += 1
            tokens = preprocess(text)
            for token in tokens:
                term_dict[token][doc_id] += 1

                # Verificar uso de memoria
                if sys.getsizeof(term_dict) >= memory_limit:
                    self._dump_block(term_dict, block_number)
                    block_number += 1
                    term_dict.clear()

        if term_dict:
            self._dump_block(term_dict, block_number)

    def _dump_block(self, term_dict, block_number):
        path = os.path.join(self.block_dir, f"block_{block_number}.pkl")
        # Guardar los términos ordenados
        sorted_dict = dict(sorted(term_dict.items()))
        with open(path, "wb") as f:
            pickle.dump(sorted_dict, f)
        print(f"[SPIMI] Bloque {block_number} guardado con {len(sorted_dict)} términos.")

    def _external_merge_blocks_with_tfidf(self):
        block_paths = [os.path.join(self.block_dir, f) for f in os.listdir(self.block_dir) if f.endswith(".pkl")]
        N = self.doc_count
        document_norms = defaultdict(float)
        
        # Función para fusionar dos bloques
        def merge_two_blocks(block1, block2):
            merged = {}
            terms = sorted(set(block1.keys()).union(block2.keys()))
            
            for term in terms:
                # Combinar postings
                combined = defaultdict(int)
                if term in block1:
                    for doc_id, freq in block1[term].items():
                        combined[doc_id] += freq
                if term in block2:
                    for doc_id, freq in block2[term].items():
                        combined[doc_id] += freq
                
                # Calcular TF-IDF y actualizar normas
                df_t = len(combined)
                idf = math.log(N / df_t) if df_t and N > 0 else 0
                postings_tfidf = []
                
                for doc_id, freq in combined.items():
                    tfidf = round(freq * idf, 5)
                    postings_tfidf.append((doc_id, tfidf))
                    document_norms[doc_id] += tfidf ** 2
                
                merged[term] = postings_tfidf
            
            return merged

        # Fusionar bloques por pares
        while len(block_paths) > 1:
            new_block_paths = []
            for i in range(0, len(block_paths), 2):
                # Buffer 1 y 2: Bloques a fusionar
                with open(block_paths[i], 'rb') as f1:
                    block1 = pickle.load(f1)
                
                block2 = {}
                if i+1 < len(block_paths):
                    with open(block_paths[i+1], 'rb') as f2:
                        block2 = pickle.load(f2)
                
                # Buffer 3: Resultado fusionado
                merged_block = merge_two_blocks(block1, block2)
                
                # Guardar bloque temporal
                new_path = os.path.join(self.block_dir, f"temp_{len(new_block_paths)}.pkl")
                with open(new_path, 'wb') as f_out:
                    pickle.dump(merged_block, f_out)
                new_block_paths.append(new_path)
            
            # Reemplazar bloques viejos con los nuevos fusionados
            block_paths = new_block_paths

        # Cargar el bloque final
        with open(block_paths[0], 'rb') as f:
            final_index = pickle.load(f)
        
        # Calcular normas finales
        document_norms = {doc_id: math.sqrt(norm) for doc_id, norm in document_norms.items()}
        
        return final_index, document_norms

    def _save_index_to_table(self, inverted_index, document_norms):
        """
        Versión corregida que asegura el formato correcto de los registros
        """
        # 1. Guardar índice invertido principal
        schema_idx = [("term", "50s"), ("postings", "text")]
        HeapFile.build_file(self.index_table_name, schema_idx, "term")
        heapfile_idx = HeapFile(self.index_table_name)

        for term, postings in inverted_index.items():
            # Convertir postings a JSON string validado
            try:
                if not isinstance(postings, str):
                    postings_json = json.dumps(postings)
                    # Validar que se puede decodificar
                    json.loads(postings_json)  # Test de decodificación
                else:
                    postings_json = postings
            except (TypeError, json.JSONDecodeError) as e:
                print(f"Error serializando postings para '{term}': {e}")
                postings_json = "[]"  # Valor por defecto seguro

            # Crear registro con tipos explícitos
            record_values = [
                str(term),          # Asegurar que es string
                str(postings_json)  # Asegurar que es string
            ]
            record = Record(schema_idx, record_values)
            heapfile_idx.insert_record(record)

        # 2. Guardar normas de documentos
        schema_norms = [("doc_id", "i"), ("norm", "f")]
        norms_table_name = f"{self.index_table_name}_norms"
        HeapFile.build_file(norms_table_name, schema_norms, "doc_id")
        heapfile_norms = HeapFile(norms_table_name)

        for doc_id, norm in document_norms.items():
            # Asegurar tipos correctos
            record_values = [
                int(doc_id),    # Asegurar que es int
                float(norm)     # Asegurar que es float
            ]
            record = Record(schema_norms, record_values)
            heapfile_norms.insert_record(record)

        # 3. Crear índices hash
        ExtendibleHashIndex.build_index(
            self.index_table_name,
            lambda field_name: heapfile_idx.extract_index(field_name),
            "term"
        )
        ExtendibleHashIndex.build_index(
            norms_table_name,
            lambda field_name: heapfile_norms.extract_index(field_name),
            "doc_id"
        )

    def _clean_blocks(self):
        """
        Elimina todos los archivos de bloques temporales.
        """
        for fname in os.listdir(self.block_dir):
            os.remove(os.path.join(self.block_dir, fname))
        os.rmdir(self.block_dir)
        print("[SPIMI] Bloques temporales eliminados.")
