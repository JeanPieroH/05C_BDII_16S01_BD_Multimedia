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
from .ExtendibleHashIndex import ExtendibleHashIndex
from storage.Record import Record
from storage.HistogramFile import HistogramFile
class SpimiAudio:
    def __init__(self, table_name: str, index_name: str, _table_path):
        self.table_name = table_name
        self.index_name = index_name
        self.index_dir = f"data/indexes/{self.table_name}/{self.index_name}"
        self.blocks_dir = f"{self.index_dir}/blocks"
        self._table_path = _table_path
        os.makedirs(self.blocks_dir, exist_ok=True)
        self.block_filenames = []
        self.block_counter = 0
        self.max_block_size = 1024 * 1024 * 1  # 1MB
        self.term_postings = {}

    def _flush_block(self):
        if not self.term_postings:
            return

        block_filename = os.path.join(self.blocks_dir, f"block_{self.block_counter}.pkl")
        with open(block_filename, 'wb') as f:
            pickle.dump(self.term_postings, f)

        self.block_filenames.append(block_filename)
        self.block_counter += 1
        self.term_postings = {}

    def build(self, histograms: list[tuple[any, dict[int, int]]]):
        # SPIMI Invert step
        for doc_id, histogram in histograms:
            for acoustic_word, tf in histogram.items():
                if acoustic_word not in self.term_postings:
                    self.term_postings[acoustic_word] = []
                self.term_postings[acoustic_word].append((doc_id, tf))

            if sys.getsizeof(self.term_postings) > self.max_block_size:
                self._flush_block()

        self._flush_block()

        # Merge step
        self._merge_blocks()

    def _merge_blocks(self):
        # 2-way merge
        while len(self.block_filenames) > 1:
            merged_blocks = []
            for i in range(0, len(self.block_filenames), 2):
                block1_filename = self.block_filenames[i]
                if i + 1 < len(self.block_filenames):
                    block2_filename = self.block_filenames[i+1]
                    merged_filename = self._merge_two_blocks(block1_filename, block2_filename)
                    merged_blocks.append(merged_filename)
                else:
                    merged_blocks.append(block1_filename)
            self.block_filenames = merged_blocks

    def _merge_two_blocks(self, block1_filename, block2_filename):
        with open(block1_filename, 'rb') as f1, open(block2_filename, 'rb') as f2:
            postings1 = pickle.load(f1)
            postings2 = pickle.load(f2)

        merged_postings = {}
        terms1 = sorted(postings1.keys())
        terms2 = sorted(postings2.keys())

        ptr1, ptr2 = 0, 0
        while ptr1 < len(terms1) and ptr2 < len(terms2):
            term1 = terms1[ptr1]
            term2 = terms2[ptr2]
            if term1 < term2:
                merged_postings[term1] = postings1[term1]
                ptr1 += 1
            elif term2 < term1:
                merged_postings[term2] = postings2[term2]
                ptr2 += 1
            else:
                merged_postings[term1] = postings1[term1] + postings2[term2]
                ptr1 += 1
                ptr2 += 1

        while ptr1 < len(terms1):
            term1 = terms1[ptr1]
            merged_postings[term1] = postings1[term1]
            ptr1 += 1

        while ptr2 < len(terms2):
            term2 = terms2[ptr2]
            merged_postings[term2] = postings2[term2]
            ptr2 += 1

        merged_filename = os.path.join(self.blocks_dir, f"merged_{self.block_counter}.pkl")
        with open(merged_filename, 'wb') as f:
            pickle.dump(merged_postings, f)

        os.remove(block1_filename)
        os.remove(block2_filename)
        self.block_counter += 1

        return merged_filename

    def _calculate_tf_idf(self, N):
        final_index_filename = self.block_filenames[0]
        with open(final_index_filename, 'rb') as f:
            final_postings = pickle.load(f)

        # Create HeapFile for the index
        index_table_name = f"{self.table_name}_{self.index_name}"
        schema_idx = [("term", "50s"), ("postings", "text")]
        HeapFile.build_file(self._table_path(index_table_name), schema_idx, "term")
        heapfile_idx = HeapFile(self._table_path(index_table_name))

        # Create HeapFile for norms
        norms_table_name = f"{index_table_name}_norms"
        schema_norms = [("doc_id", "i"), ("norm", "f")]
        HeapFile.build_file(self._table_path(norms_table_name), schema_norms, "doc_id")
        heapfile_norms = HeapFile(self._table_path(norms_table_name))

        document_norms = defaultdict(float)

        for term, postings in final_postings.items():
            dft = len(postings)
            idf = math.log10(N / dft) if dft > 0 else 0

            postings_tfidf = []
            for hist_id, tf in postings:
                w = math.log10(1 + tf) * idf
                postings_tfidf.append((hist_id, w))
                document_norms[hist_id] += w ** 2

            postings_json = json.dumps(postings_tfidf)
            record = Record(schema_idx, [str(term), postings_json])
            heapfile_idx.insert_record_free(record)

        for doc_id, norm_sum in document_norms.items():
            norm = math.sqrt(norm_sum)
            record = Record(schema_norms, [doc_id, norm])
            heapfile_norms.insert_record(record)

        ExtendibleHashIndex.build_index(
            self._table_path(index_table_name),
            lambda field_name: heapfile_idx.extract_index(field_name),
            "term"
        )
        ExtendibleHashIndex.build_index(
            self._table_path(norms_table_name),
            lambda field_name: heapfile_norms.extract_index(field_name),
            "doc_id"
        )

        os.remove(final_index_filename)
        os.rmdir(self.blocks_dir)
