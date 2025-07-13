import struct
import math
import os
import json
from typing import Union, List, Optional, BinaryIO
from .IndexRecord import IndexRecord
from . import utils

class SequentialIndex:
    METADATA_FORMAT = "iii"  # main_size, aux_size, max_aux_size
    METADATA_SIZE = struct.calcsize(METADATA_FORMAT)

    def __init__(self, table_name: str, indexed_field: str):

        if not os.path.exists(table_name + "." + indexed_field + ".seq.idx"):
            raise FileNotFoundError(f"El índice para {table_name} no existe. Crea el índice primero.")

        self.filename = table_name + "." + indexed_field + ".seq.idx"
        
        # Cargar schema y determinar formato de la clave
        self.schema = utils.load_schema(table_name)
        self.key_field = indexed_field
        self.key_format = utils.get_key_format_from_schema(self.schema, self.key_field)
        
        # Tamaño dinámico de registro basado en el formato
        sample_record = IndexRecord(self.key_format, utils.get_default_key(self.key_format), 0)
        self.record_size = sample_record.size
        
        with open(self.filename, "rb+") as f:
            meta = f.read(self.METADATA_SIZE)
            self.main_size, self.aux_size, self.max_aux_size = struct.unpack(self.METADATA_FORMAT, meta)
    
    @staticmethod
    def build_index(heap_filename: str, extract_index_fn, key_field: str):
        """
        Construye un nuevo índice secuencial para el campo especificado.
        """
        # Cargar schema para validación
        schema_file = heap_filename + ".schema.json"
        with open(schema_file, "r") as f:
            schema = json.load(f)
        
        # Obtener formato del campo
        key_format = next(f["type"] for f in schema["fields"] if f["name"] == key_field)
        
        # Generar nombre de archivo de índice
        base, _ = os.path.splitext(heap_filename)
        idx_filename = f"{base}.{key_field}.seq.idx"

        # Obtener y validar entradas
        entries = extract_index_fn(key_field)
        valid_entries = [(k, o) for k, o in entries if SequentialIndex._validate_type(k, key_format)]
        
        # Ordenar según el tipo de dato
        valid_entries.sort(key=lambda x: x[0])

        main_size = len(valid_entries)
        aux_size = 0
        max_aux_size = max(1, math.floor(math.log2(main_size))) if main_size > 0 else 1

        # Crear archivo de índice
        with open(idx_filename, "wb") as f:
            # Escribir metadatos
            f.write(struct.pack(SequentialIndex.METADATA_FORMAT, main_size, aux_size, max_aux_size))
            
            # Escribir registros
            for key, offset in valid_entries:
                rec = IndexRecord(key_format, key, offset)
                f.write(rec.pack())
            
            # Escribir área auxiliar vacía
            empty_rec = IndexRecord(key_format, 
                                  -1 if key_format == 'i' else '' if 's' in key_format else -1.0, 
                                  0)
            for _ in range(max_aux_size):
                f.write(empty_rec.pack())
        
        return True

    @staticmethod
    def _validate_type(value, format: str) -> bool:
        """Valida que el valor coincida con el formato esperado"""
        if format == 'i':
            return isinstance(value, int)
        elif format == 'f':
            return isinstance(value, float)
        elif 's' in format:
            return isinstance(value, str)
        return False

    def _compare_keys(self, a, b) -> int:
        """Compara dos claves devolviendo -1, 0 o 1"""
        if a == b:
            return 0
        return -1 if a < b else 1

    def update_metadata(self, file_handle=None):
        """Actualiza los metadatos en el archivo."""
        if file_handle:
            file_handle.seek(0)
            file_handle.write(struct.pack(self.METADATA_FORMAT, self.main_size, self.aux_size, self.max_aux_size))
        else:
            with open(self.filename, "r+b") as f:
                f.seek(0)
                f.write(struct.pack(self.METADATA_FORMAT, self.main_size, self.aux_size, self.max_aux_size))

    def insert_record(self, record: IndexRecord):
        """Inserta un nuevo registro en el área auxiliar."""
        if record.format != self.key_format:
            raise TypeError(f"El registro tiene formato {record.format}, se esperaba {self.key_format}")

        with open(self.filename, "r+b") as f:
            # Posicionarse al final del área auxiliar
            pos = self.METADATA_SIZE + (self.main_size + self.aux_size) * self.record_size
            f.seek(pos)
            
            # Escribir el registro
            f.write(record.pack())
            self.aux_size += 1
            self.update_metadata(file_handle=f)
            #print("Indice Secuencial: Registro añadido correctamente")

        # Reconstruir si el área auxiliar es demasiado grande
        if self.aux_size > self.max_aux_size:
            #print("Indice Secuencial: Area Auxiliar llena, reconstruyendo...")
            self.rebuild_file()

    def rebuild_file(self):
        """Reconstruye el archivo fusionando áreas principal y auxiliar."""
        all_recs = []
        
        # Leer todos los registros válidos
        with open(self.filename, "rb") as f:
            # Leer área principal
            f.seek(self.METADATA_SIZE)
            for _ in range(self.main_size):
                data = f.read(self.record_size)
                if not data:
                    break
                rec = IndexRecord.unpack(data, self.key_format)
                if not self._is_deleted(rec):
                    all_recs.append(rec)
            
            # Leer área auxiliar
            f.seek(self.METADATA_SIZE + self.main_size * self.record_size)
            for _ in range(self.aux_size):
                data = f.read(self.record_size)
                if not data:
                    break
                rec = IndexRecord.unpack(data, self.key_format)
                if not self._is_deleted(rec):
                    all_recs.append(rec)

        # Ordenar registros
        all_recs.sort(key=lambda r: r.key)

        # Calcular nuevos parámetros
        new_main = len(all_recs)
        new_aux_max = max(1, math.floor(math.log2(new_main))) if new_main > 0 else 1
        empty_rec = utils.get_empty_record(self.key_format)

        # Escribir archivo temporal
        tmp_file = self.filename + ".tmp"
        with open(tmp_file, "wb") as f:
            # Escribir metadatos
            f.write(struct.pack(self.METADATA_FORMAT, new_main, 0, new_aux_max))
            
            # Escribir registros
            for rec in all_recs:
                f.write(rec.pack())
            
            # Escribir área auxiliar vacía
            for _ in range(new_aux_max):
                f.write(empty_rec.pack())

        # Reemplazar archivo original
        os.replace(tmp_file, self.filename)
        
        # Actualizar estado
        self.main_size = new_main
        self.aux_size = 0
        self.max_aux_size = new_aux_max

    def _is_deleted(self, record: IndexRecord) -> bool:
        """Determina si un registro está marcado como eliminado."""
        if self.key_format == 'i':
            return record.key == -1
        elif self.key_format == 'f':
            return record.key == -1.0
        elif 's' in self.key_format:
            return record.key == ''
        return False

    def search_record(self, key: Union[int, float, str]) -> List[IndexRecord]:
        """Devuelve todos los (key, offset) – tanto en área principal como auxiliar."""
        if not self._validate_type(key, self.key_format):
            raise TypeError(f"Clave {key!r} no es del tipo {self.key_format}")

        results: List[IndexRecord] = []

        with open(self.filename, "rb") as f:
            # ---------- 1) bin-search en área principal ----------
            lo, hi = 0, self.main_size - 1
            pos = -1
            while lo <= hi:
                mid = (lo + hi) // 2
                f.seek(self.METADATA_SIZE + mid * self.record_size)
                rec = IndexRecord.unpack(f.read(self.record_size), self.key_format)

                cmp = self._compare_keys(rec.key, key)
                if cmp == 0:
                    pos = mid
                    break
                elif cmp < 0:
                    lo = mid + 1
                else:
                    hi = mid - 1

            if pos != -1:
                # --- 1a) retroceder hasta el primer duplicado ---
                i = pos
                while i >= 0:
                    f.seek(self.METADATA_SIZE + i * self.record_size)
                    rec = IndexRecord.unpack(f.read(self.record_size), self.key_format)
                    if self._compare_keys(rec.key, key) != 0:
                        break
                    if not self._is_deleted(rec):
                        results.append(rec)
                    i -= 1

                # --- 1b) avanzar hacia la derecha ---
                i = pos + 1
                while i < self.main_size:
                    f.seek(self.METADATA_SIZE + i * self.record_size)
                    rec = IndexRecord.unpack(f.read(self.record_size), self.key_format)
                    if self._compare_keys(rec.key, key) != 0:
                        break
                    if not self._is_deleted(rec):
                        results.append(rec)
                    i += 1

            # ---------- 2) barrer área auxiliar ----------
            f.seek(self.METADATA_SIZE + self.main_size * self.record_size)
            for _ in range(self.aux_size):
                data = f.read(self.record_size)
                if not data:
                    break
                rec = IndexRecord.unpack(data, self.key_format)
                if rec.key == key and not self._is_deleted(rec):
                    results.append(rec)

        return results

    def delete_record(self, key, offset) -> bool:
        """Marca como eliminado el (key, offset) que coincida; devuelve True si lo encontró."""
        if not self._validate_type(key, self.key_format):
            raise TypeError(f"Clave {key} no es del tipo {self.key_format}")

        empty = utils.get_empty_record(self.key_format)
        found = False

        with open(self.filename, "r+b") as f:
            # --- área principal ---
            f.seek(self.METADATA_SIZE)
            for _ in range(self.main_size):
                pos = f.tell()
                rec = IndexRecord.unpack(f.read(self.record_size), self.key_format)
                if rec.key == key and rec.offset == offset:
                    f.seek(pos)
                    f.write(empty.pack())
                    found = True
                    break

            # --- área auxiliar ---
            if not found:
                f.seek(self.METADATA_SIZE + self.main_size * self.record_size)
                for _ in range(self.aux_size):
                    pos = f.tell()
                    rec = IndexRecord.unpack(f.read(self.record_size), self.key_format)
                    if rec.key == key and rec.offset == offset:
                        f.seek(pos)
                        f.write(empty.pack())
                        found = True
                        break
        return found

    def search_range(self, start_key: Union[int, float, str], end_key: Union[int, float, str]) -> List[IndexRecord]:
        """Busca registros cuyo key esté en el rango [start_key, end_key]."""
        if not self._validate_type(start_key, self.key_format) or not self._validate_type(end_key, self.key_format):
            raise TypeError("Las claves del rango no coinciden con el tipo de índice")

        results = []
        with open(self.filename, "rb") as f:
            # Encontrar primer registro en rango (búsqueda binaria)
            low, high = 0, self.main_size - 1
            first_pos = 0
            
            while low <= high:
                mid = (low + high) // 2
                f.seek(self.METADATA_SIZE + mid * self.record_size)
                rec = IndexRecord.unpack(f.read(self.record_size), self.key_format)
                
                if self._compare_keys(rec.key, start_key) < 0:
                    low = mid + 1
                else:
                    high = mid - 1
                    first_pos = mid

            # Leer registros en rango del área principal
            f.seek(self.METADATA_SIZE + first_pos * self.record_size)
            for _ in range(first_pos, self.main_size):
                data = f.read(self.record_size)
                if not data:
                    break
                rec = IndexRecord.unpack(data, self.key_format)
                if self._compare_keys(rec.key, start_key) < 0:
                    continue
                if self._compare_keys(rec.key, end_key) > 0:
                    break
                if not self._is_deleted(rec):
                    results.append(rec)

            # Buscar en área auxiliar
            f.seek(self.METADATA_SIZE + self.main_size * self.record_size)
            for _ in range(self.aux_size):
                data = f.read(self.record_size)
                if not data:
                    break
                rec = IndexRecord.unpack(data, self.key_format)
                if self._compare_keys(rec.key, start_key) >= 0 and self._compare_keys(rec.key, end_key) <= 0:
                    if not self._is_deleted(rec):
                        results.append(rec)

        return results

    def print_all(self):
        """Imprime todos los registros del índice."""
        with open(self.filename, "rb") as f:
            # Leer metadatos
            meta = f.read(self.METADATA_SIZE)
            ms, au, ma = struct.unpack(self.METADATA_FORMAT, meta)
            print(f"Metadata: Main={ms}, Aux={au}, MaxAux={ma}")
            print("Type:", self.key_format)
            print("-" * 50)
            
            # Leer área principal
            print("[MAIN AREA]")
            for i in range(ms):
                data = f.read(self.record_size)
                if not data:
                    break
                rec = IndexRecord.unpack(data, self.key_format)
                status = "DELETED" if self._is_deleted(rec) else "ACTIVE"
                print(f"Pos {i}: Key={rec.key}, Offset={rec.offset} [{status}]")
            
            # Leer área auxiliar
            print("\n[AUX AREA]")
            for i in range(au):
                data = f.read(self.record_size)
                if not data:
                    break
                rec = IndexRecord.unpack(data, self.key_format)
                status = "DELETED" if self._is_deleted(rec) else "ACTIVE"
                print(f"Pos {i}: Key={rec.key}, Offset={rec.offset} [{status}]")