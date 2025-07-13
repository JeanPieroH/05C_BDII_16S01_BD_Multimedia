import os
import math
from rtree import index
import json
from typing import Union, List, Tuple
from .IndexRecord import IndexRecord, re_tuple
from . import utils
from storage.HeapFile import HeapFile

class RTreeIndex:
    def __init__(self, table_path: str, indexed_field: str):
        # Save table_path and indexed_field
        self.table_path = table_path
        self.indexed_field = indexed_field

        # Validate schema and field
        self.schema = utils.load_schema(table_path)
        if indexed_field not in [f["name"] for f in self.schema["fields"]]:
            raise ValueError(f"Campo {indexed_field} no existe en el schema de {table_path}")
    
        self.filename = f"{table_path}.{indexed_field}.rtree"

        # Validate if index file exists
        if not (os.path.exists(self.filename + ".idx") and os.path.exists(self.filename + ".dat")):
            raise FileNotFoundError(f"El índice RTree para {table_path} en el campo {indexed_field} no existe. Crea el índice primero.")
        
        self.key_format = utils.get_key_format_from_schema(self.schema, indexed_field)
        self.dims = int(self.key_format[:-1])

        # Load index using library
        props = index.Property()
        props.storage = index.RT_Disk
        self.idx = index.Index(self.filename, properties = props)

    @staticmethod
    def build_index(heap_path: str, extract_index_fn, key_field: str) -> bool:
        # Load schema
        schema = utils.load_schema(heap_path)
        # Get key format
        key_format: str = utils.get_key_format_from_schema(schema, key_field)
        
        # Generate index filename
        base, _ = os.path.splitext(heap_path)
        idx_filename = f"{base}.{key_field}.rtree"

        for ext in [".idx", ".dat"]:
            if os.path.exists(idx_filename + ext):
                os.remove(idx_filename + ext)

        # Extract and validate entries
        entries = extract_index_fn(key_field)
        valid_entries = [(RTreeIndex.to_mbr(k), o) for k, o in entries if RTreeIndex.validate_type(k, key_format)]
        
        # Create index using library
        props = index.Property()
        props.storage = index.RT_Disk
        idx = index.Index(idx_filename, properties = props)

        # Write registers
        for key, offset in valid_entries:
            if not isinstance(offset, int):
                raise ValueError(f"Offset inválido: {offset}")
            idx.insert(offset, key)

        idx.close()
        return True

    @staticmethod
    def validate_type(value: Tuple[Union[int, float], ...], format: str) -> bool:
        m = re_tuple.fullmatch(format)
        if not m:
            return False
        
        n, type_char = int(m.group(1)), m.group(2)
        if n not in (2, 3, 4, 6) or not isinstance(value, tuple) or len(value) != n:
            return False
        
        if type_char == 'i':
            return all(isinstance(x, int) for x in value)
        
        return  all(isinstance(x, float) for x in value)

    @staticmethod
    def to_mbr(value: Tuple[Union[int, float], ...]) -> tuple:
        if len(value) == 2:
            x, y = value
            return (x, y, x, y)
        if len(value) == 3:
            x, y, z = value
            return (x, y, z, x, y, z)
        else:
            return value

    @staticmethod
    def point_mbr_mindist(point: Tuple[Union[int, float], ...], mbr: Tuple[Union[int, float], ...]) -> float:
        n = len(point)
        dist = 0.0
        for i in range(n):
            q = point[i]   
            l = mbr[i]
            u = mbr[i + n]
            if q < l:
                dist += (q - l)**2
            elif q > u:
                dist += (q - u)**2
        return math.sqrt(dist)

    @staticmethod
    def euclidean_distance(a: Tuple[Union[int, float], ...], b: Tuple[Union[int, float], ...]) -> float:
        n = len(a)
        dist = 0.0
        for i in range(n):
            dist += (a[i] - b[i])**2
        return math.sqrt(dist)

    def validate_point(self, point: Tuple[Union[int, float], ...]):
        if self.dims in (2, 4):
            expected = 2
        elif self.dims in (3, 6):
            expected = 3
        else:
            raise ValueError(f"Formato inválido: {self.key_format}")
        if not (isinstance(point, tuple) and len(point) == expected):
            raise TypeError(f"Punto {point!r} debe ser tupla de {expected} dimensiones")
        if not all(isinstance(v, (int, float)) for v in point):
            raise TypeError(f"Componentes de {point!r} deben ser int o float")

    def insert_record(self, record: IndexRecord):
        if not isinstance(record, IndexRecord):
            raise TypeError("Se esperaba un objeto IndexRecord")

        if self.key_format != record.format:
            raise TypeError(f"El registro tiene formato {record.format}, se esperaba {self.key_format}")
        
        if not isinstance(record.key, tuple):
            raise TypeError(f"El campo del registro es de tipo {type(record.key)}, se esperaba una tupla numérica")

        if not isinstance(record.offset, int):
            raise TypeError(f"Offset inválido (debe ser int), se recibió {type(record.offset)}")
        
        bounds = self.to_mbr(record.key)
        self.idx.insert(record.offset, bounds)
    
    def search_record(self, point: Tuple[Union[int, float], ...]) -> List[IndexRecord]:
        if not self.validate_type(point, self.key_format):
            raise TypeError(f"La clave debe ser tupla de números con formato {self.key_format}")

        bounds = self.to_mbr(point)
        offsets = list(self.idx.intersection(bounds))
        return [IndexRecord(self.key_format, point, offset) for offset in offsets]

    def search_radius(self, point: Tuple[Union[int, float], ...], radius: float) -> List[IndexRecord]:
        self.validate_point(point)
        point = tuple(float(c) for c in point)
        mins = tuple(c - radius for c in point)
        maxs = tuple(c + radius for c in point)
        bounds = mins + maxs

        offsets = list(self.idx.intersection(bounds))

        heap_file = HeapFile(self.table_path)
        field_names = [name for name, _ in heap_file.schema]
        key_pos = field_names.index(self.indexed_field)

        results: List[IndexRecord] = []
        isbox = self.dims in (4, 6)

        for offset in offsets:
            record = heap_file.fetch_record_by_offset(offset)
            key_val = record.values[key_pos]
            dist = 0.0
            if isbox:
                dist = RTreeIndex.point_mbr_mindist(point, key_val)

            else:
                dist = RTreeIndex.euclidean_distance(point, key_val)
            
            if dist <= radius:
                results.append(IndexRecord(self.key_format, key_val, offset))

        return results
    
    def search_bounds(self, lower_bound: Tuple[Union[int, float], ...], upper_bound: Tuple[Union[int, float], ...]) -> List[IndexRecord]:
        self.validate_point(lower_bound)
        self.validate_point(upper_bound)
        lower_bound = tuple(float(c) for c in lower_bound)
        upper_bound = tuple(float(c) for c in upper_bound)
        bounds = lower_bound + upper_bound

        offsets = list(self.idx.intersection(bounds))

        heap_file = HeapFile(self.table_path)
        field_names = [name for name, _ in heap_file.schema]
        key_pos = field_names.index(self.indexed_field)

        results: List[IndexRecord] = []

        for offset in offsets:
            record = heap_file.fetch_record_by_offset(offset)
            key_val = record.values[key_pos]
            results.append(IndexRecord(self.key_format, key_val, offset))

        return results

    def search_knn(self, point: Tuple[Union[int, float], ...], k: int) -> List[IndexRecord]:
        self.validate_point(point)
        point = self.to_mbr(point)
        offsets = self.idx.nearest(point, num_results = k)

        heap_file = HeapFile(self.table_path)
        field_names = [name for name, _ in heap_file.schema]
        key_pos = field_names.index(self.indexed_field)

        results: List[IndexRecord] = []

        for offset in offsets:
            record = heap_file.fetch_record_by_offset(offset)
            key_val = record.values[key_pos]
            results.append(IndexRecord(self.key_format, key_val, offset))

        return results

    def delete_record(self, key: Tuple[Union[int, float], ...], offset: int) -> bool:
        if not RTreeIndex.validate_type(key, self.key_format):
            raise TypeError(f"La clave debe ser tupla de números")
        if not isinstance(offset, int):
            raise TypeError(f"El offset debe ser int")
        
        bounds = self.to_mbr(key)

        try:
            self.idx.delete(id = offset, coordinates = bounds)
            return True
        except Exception as e:
            return False
        
    def print_all(self):
        offsets = self.idx.intersection(tuple((float('-inf'),) * self.dims + (float('inf'),) * self.dims))
        heap_file = HeapFile(self.table_path)
        field_names = [name for name, _ in heap_file.schema]
        key_pos = field_names.index(self.indexed_field)

        results: List[IndexRecord] = []
        for offset in offsets:
            record = heap_file.fetch_record_by_offset(offset)
            key_val = record.values[key_pos]
            results.append(IndexRecord(self.key_format, key_val, offset))
        
        for rec in results:
            print(f"Offset: {rec.offset}, Key: {rec.key}")