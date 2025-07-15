import os
import pickle
import math
from collections import defaultdict
import sys
import json
import heapq
from typing import Dict, List, Tuple, DefaultDict, Any, Union, Iterator

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from storage.HeapFile import HeapFile
from storage.HistogramFile import HistogramFile
from .ExtendibleHashIndex import ExtendibleHashIndex
from storage.Record import Record

class SpimiAudioIndexer:
    def __init__(self, _table_path, field_name: str, block_dir: str = "audio_index_blocks", index_table_name: str = "acoustic_index"):
        self.block_dir = os.path.join("backend/database/tables", block_dir)
        self.index_table_name = index_table_name
        self._table_path = _table_path
        self.field_name = field_name
        os.makedirs(self.block_dir, exist_ok=True)

    def build_index(self, table_name: str) -> None:
        self.doc_count = 0
        self._process_documents(table_name)
        self._streaming_merge_with_tfidf()
        self._clean_blocks()

    def _process_documents(self, table_name: str) -> None:
        heapfile = HeapFile(self._table_path(table_name))
        histogram_handler = HistogramFile(self._table_path(table_name), self.field_name)

        term_dict = defaultdict(lambda: defaultdict(int))
        block_number = 0
        memory_limit = 4 * 1024  # 4 KB

        for record in heapfile.get_all_records():
            self.doc_count += 1
            _, histogram_offset = record.values[heapfile.schema.index((self.field_name, "SOUND"))]

            if histogram_offset == -1:
                continue

            histogram = histogram_handler.read(histogram_offset)

            for centroid_id, count in histogram:
                term_dict[centroid_id][record.values[0]] += count # values[0] is the id

                if sys.getsizeof(term_dict) >= memory_limit:
                    self._dump_block(term_dict, block_number)
                    block_number += 1
                    term_dict.clear()

        if term_dict:
            self._dump_block(term_dict, block_number)
        self.total_blocks = block_number + 1

    def _dump_block(self, term_dict: Dict[int, Dict[int, int]], block_number: int) -> None:
        path = os.path.join(self.block_dir, f"block_{block_number}.json")
        sorted_dict = dict(sorted(term_dict.items()))
        with open(path, "w") as f:
            json.dump(sorted_dict, f)

    def _streaming_merge_with_tfidf(self) -> None:
        """Merge externo con streaming que evita cargar todo en RAM"""
        # 1. Inicializar el archivo final de índice
        schema_idx = [("term", "i"), ("postings", "text")]
        HeapFile.build_file(self._table_path(self.index_table_name), schema_idx, "term")
        heapfile_idx = HeapFile(self._table_path(self.index_table_name))

        # 2. Inicializar el archivo de normas
        schema_norms = [("doc_id", "i"), ("norm", "f")]
        norms_table_name = f"{self.index_table_name}_norms"
        HeapFile.build_file(self._table_path(norms_table_name), schema_norms, "doc_id")
        heapfile_norms = HeapFile(self._table_path(norms_table_name))

        # 3. Inicializar estructuras para el merge
        block_paths = [os.path.join(self.block_dir, f) for f in os.listdir(self.block_dir) if f.endswith(".json")]
        block_iters = []
        current_terms = {}
        heap = []
        document_norms = defaultdict(float)
        N = self.doc_count

        # 4. Crear iteradores para cada bloque
        for i, path in enumerate(block_paths):
            with open(path, "r") as f:
                block = json.load(f)
                block_iter = iter(block.items())
                block_iters.append(block_iter)
                try:
                    term, postings = next(block_iter)
                    heapq.heappush(heap, (int(term), i))
                    current_terms[i] = (int(term), postings)
                except StopIteration:
                    continue

        # 5. Proceso de merge por streaming
        while heap:
            term, block_idx = heapq.heappop(heap)

            # Combinar todas las ocurrencias del término
            combined_freqs = defaultdict(int)
            blocks_to_advance = []

            for i in list(current_terms.keys()):
                current_term, freqs = current_terms[i]
                if current_term == term:
                    for doc_id, count in freqs.items():
                        combined_freqs[int(doc_id)] += count
                    blocks_to_advance.append(i)
                    del current_terms[i]

            # Calcular TF-IDF
            df = len(combined_freqs)
            idf = math.log10(N / df) if df and N > 0 else 0
            postings_tfidf = []

            for doc_id, freq in combined_freqs.items():
                tf = 1 + math.log10(freq) if freq > 0 else 0
                tfidf = round(tf * idf, 5)
                postings_tfidf.append((doc_id, tfidf))
                document_norms[doc_id] += tfidf ** 2

            # Guardar término en el índice final (streaming)
            postings_json = json.dumps([[doc_id, tfidf] for doc_id, tfidf in postings_tfidf])
            record = Record(schema_idx, [term, postings_json])
            heapfile_idx.insert_record_free(record)

            # Avanzar los bloques procesados
            for i in blocks_to_advance:
                try:
                    next_term, next_postings = next(block_iters[i])
                    heapq.heappush(heap, (int(next_term), i))
                    current_terms[i] = (int(next_term), next_postings)
                except StopIteration:
                    # Fin de este bloque
                    pass

        # 6. Guardar normas (streaming)
        for doc_id, norm_sum in document_norms.items():
            norm = math.sqrt(norm_sum)
            record = Record(schema_norms, [doc_id, norm])
            heapfile_norms.insert_record(record)

        # 7. Crear índices hash
        ExtendibleHashIndex.build_index(
            self._table_path(self.index_table_name),
            lambda field_name: heapfile_idx.extract_index(field_name),
            "term"
        )
        ExtendibleHashIndex.build_index(
            self._table_path(norms_table_name),
            lambda field_name: heapfile_norms.extract_index(field_name),
            "doc_id"
        )

    def _clean_blocks(self):
        if os.path.exists(self.block_dir):
            for fname in os.listdir(self.block_dir):
                os.remove(os.path.join(self.block_dir, fname))
            os.rmdir(self.block_dir)
