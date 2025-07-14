import struct
import json
import os
from typing import Iterator, Optional, Tuple, List
import pandas as pd

from .Record import Record
from .TextFile import TextFile

# --------------------------------------------------------
#  Valores centinela para marcar registros eliminados
# --------------------------------------------------------
SENTINEL_INT: int = -1  # Para campos int / long
SENTINEL_FLOAT: float = float("-inf")  # Para campos float / double
SENTINEL_STR: str = ""  # Para campos string

# --------------------------------------------------------
#  Constantes internas
# --------------------------------------------------------
PTR_SIZE = 4  # int32 para enlazar free‑list
METADATA_FORMAT = "ii"  # [heap_size, free_head]
METADATA_SIZE = struct.calcsize(METADATA_FORMAT)


class HeapFile:
    """Archivo heap con clave primaria opcional y free‑list interna.

    • Cada *slot* = datos de Record + 4 bytes (next_free).
    • Cuando un slot está libre: PK = centinela y next_free apunta al
      siguiente hueco (o -1 si es el último).
    • Offsets lógicos nunca cambian, así los índices externos se mantienen.
    """

    # ------------------------------------------------------------------
    # Creación del archivo ---------------------------------------------
    # ------------------------------------------------------------------
    @staticmethod
    def build_file(
        table_name: str,
        schema: List[Tuple[str, str]],
        primary_key: Optional[str] = None,
    ) -> None:
        """Crea archivo <table_name>.dat y <table_name>.schema.json."""
        filename = table_name + ".dat"
        with open(filename, "wb") as f:
            f.write(struct.pack(METADATA_FORMAT, 0, -1))  # heap_size=0, free_head=-1

        schema_file = table_name + ".schema.json"
        fields = [
            {"name": n, "type": fmt, "is_primary_key": (n == primary_key)}
            for n, fmt in schema
        ]
        with open(schema_file, "w", encoding="utf-8") as jf:
            json.dump(
                {"table_name": os.path.basename(table_name), "fields": fields},
                jf,
                indent=4,
            )

        # Crear archivo .text por cada campo tipo "text"
        for field_name, fmt in schema:
            if fmt == "text":
                TextFile.build_file(table_name, field_name)

    # ------------------------------------------------------------------
    # Inicialización ----------------------------------------------------
    # ------------------------------------------------------------------
    def __init__(self, table_name: str):
        self.table_name = table_name.split("/")[
            -1
        ]  # saves table name for stuff in parser
        self.filename = table_name + ".dat"
        self.schema, self.primary_key = self._load_schema(self.filename)
        self.rec_data_size = Record.get_size(self.schema)
        self.slot_size = self.rec_data_size + PTR_SIZE

        if not os.path.exists(self.filename):
            raise FileNotFoundError(
                f"{self.filename} no existe. Cree la tabla primero."
            )

        with open(self.filename, "rb") as f:
            self.heap_size, self.free_head = struct.unpack(
                METADATA_FORMAT, f.read(METADATA_SIZE)
            )

    # ------------------------------------------------------------------
    # Utilidades internas ----------------------------------------------
    # ------------------------------------------------------------------
    def _load_schema(self, fname) -> Tuple[List[Tuple[str, str]], Optional[str]]:
        with open(fname.replace(".dat", ".schema.json"), encoding="utf-8") as jf:
            js = json.load(jf)
        schema = [(fld["name"], fld["type"]) for fld in js["fields"]]
        pk = next(
            (fld["name"] for fld in js["fields"] if fld.get("is_primary_key")), None
        )
        return schema, pk

    def _pk_idx_fmt(self) -> Tuple[int, str]:
        if self.primary_key is None:
            raise ValueError("La tabla no tiene clave primaria definida.")
        for i, (n, fmt) in enumerate(self.schema):
            if n == self.primary_key:
                return i, fmt
        raise RuntimeError("Inconsistencia en schema: PK no encontrada.")

    @staticmethod
    def _sentinel(fmt: str):
        if "s" in fmt:
            return SENTINEL_STR
        if fmt[-1] in "fd":
            return SENTINEL_FLOAT
        return SENTINEL_INT

    def _write_header(self, fh):
        fh.seek(0)
        fh.write(struct.pack(METADATA_FORMAT, self.heap_size, self.free_head))

    # ------------------------------------------------------------------
    # Inserción ---------------------------------------------------------
    # ------------------------------------------------------------------

    def _process_text_fields(self, record: Record) -> None:
        for idx, (field_name, fmt) in enumerate(record.schema):
            if fmt == "text":
                text_value = record.values[idx]
                text_file = TextFile(self.table_name, field_name)
                offset = text_file.insert(text_value)
                record.values[idx] = offset  # reemplazar texto por offset

    def insert_record(self, record: Record) -> int:
        if record.schema != self.schema:
            raise ValueError("Esquema del registro no coincide.")

        # ── 1. Verificar unicidad de PK en una SOLA pasada ─────────────
        if self.primary_key:
            pk_idx, pk_fmt = self._pk_idx_fmt()
            pk_val = record.values[pk_idx]
            if pk_val == self._sentinel(pk_fmt):
                raise ValueError("Valor centinela no permitido en PK.")

            with open(self.filename, "rb") as fh:  # una sola apertura
                fh.seek(METADATA_SIZE)
                for _ in range(self.heap_size):
                    buf = fh.read(self.rec_data_size)
                    if len(buf) < self.rec_data_size:
                        break
                    if Record.unpack(buf, self.schema).values[pk_idx] == pk_val:
                        raise ValueError(f"PK duplicada: {pk_val}")
                    fh.seek(PTR_SIZE, os.SEEK_CUR)  # saltar next_free

        self._process_text_fields(record)

        # ── 2. Insertar (reciclar hueco o append) ─────────────────────
        with open(self.filename, "r+b") as fh:
            if self.free_head == -1:  # sin huecos → append
                slot_off = self.heap_size
                fh.seek(0, os.SEEK_END)
                fh.write(record.pack())
                fh.write(struct.pack("i", 0))  # next_free = 0
                self.heap_size += 1
            else:  # reciclar hueco
                slot_off = self.free_head
                byte_off = METADATA_SIZE + slot_off * self.slot_size
                fh.seek(byte_off + self.rec_data_size)
                self.free_head = struct.unpack("i", fh.read(4))[0]  # siguiente libre
                fh.seek(byte_off)
                fh.write(record.pack())
                fh.write(struct.pack("i", 0))
            self._write_header(fh)  # actualizar cabecera
            print("Registro:", record, " insertado correctamente")
            return slot_off

    def insert_record_free(self, record: Record) -> int:
        """Inserta un registro sin verificar unicidad de PK. Usa free-list si hay huecos."""
        if record.schema != self.schema:
            raise ValueError("Esquema del registro no coincide.")

        self._process_text_fields(record)

        with open(self.filename, "r+b") as fh:
            if self.free_head == -1:  # sin huecos → append
                slot_off = self.heap_size
                fh.seek(0, os.SEEK_END)
                fh.write(record.pack())
                fh.write(struct.pack("i", 0))  # next_free = 0
                self.heap_size += 1
            else:  # reciclar hueco
                slot_off = self.free_head
                byte_off = METADATA_SIZE + slot_off * self.slot_size
                fh.seek(byte_off + self.rec_data_size)
                self.free_head = struct.unpack("i", fh.read(4))[0]  # siguiente libre
                fh.seek(byte_off)
                fh.write(record.pack())
                fh.write(struct.pack("i", 0))

            self._write_header(fh)
            print(
                "Registro (sin restricción PK):",
                record,
                "insertado en offset",
                slot_off,
            )
            return slot_off

    # ------------------------------------------------------------------
    # Borrado -----------------------------------------------------------
    # ------------------------------------------------------------------
    def delete_by_pk(self, key) -> Tuple[bool, int, Optional[Record]]:
        if self.primary_key is None:
            raise ValueError("Tabla sin clave primaria.")
        pk_idx, pk_fmt = self._pk_idx_fmt()
        sentinel = self._sentinel(pk_fmt)

        with open(self.filename, "r+b") as fh:
            for pos in range(self.heap_size):
                byte_off = METADATA_SIZE + pos * self.slot_size
                fh.seek(byte_off)
                buf = fh.read(self.rec_data_size)
                rec = Record.unpack(buf, self.schema)
                old_rec = Record.unpack(buf, self.schema)
                if rec.values[pk_idx] != key:
                    continue
                # Borrar campos tipo text
                for i, (field_name, fmt) in enumerate(self.schema):
                    if fmt == "text":
                        offset = old_rec.values[i]
                        TextFile(self.table_name, field_name).delete(offset)
                # marcar hueco: set PK = sentinel y next_free = free_head
                rec.values[pk_idx] = sentinel
                fh.seek(byte_off)
                fh.write(rec.pack())
                fh.write(struct.pack("i", self.free_head))
                self.free_head = pos
                self._write_header(fh)
                print(
                    "Registro con PK:",
                    key,
                    "con contenido:",
                    old_rec,
                    "borrado correctamente",
                )
                return True, pos, old_rec
        return False, -1, None

    # ------------------------------------------------------------------
    #  Búsqueda secuencial por cualquier campo --------------------------
    # ------------------------------------------------------------------
    def search_by_field(self, field: str, value):
        """
        Busca secuencialmente en el heap y devuelve:
          • Una lista de Record con todas las coincidencias.
          • Si el campo es la clave primaria, la búsqueda se detiene
            tras la primera coincidencia.

        Si el campo no existe, lanza KeyError.
        """
        # --- validar campo -------------------------------------------------
        names = [n for n, _ in self.schema]
        if field not in names:
            raise KeyError(f"Campo '{field}' no existe en el esquema.")
        fld_idx = names.index(field)

        # --- info de PK/tombstone -----------------------------------------
        pk_idx, pk_sentinel = None, None
        stop_early = False
        if self.primary_key is not None:
            pk_idx, pk_fmt = self._pk_idx_fmt()
            pk_sentinel = self._sentinel(pk_fmt)
            stop_early = field == self.primary_key

        resultados = []

        with open(self.filename, "rb") as fh:
            fh.seek(METADATA_SIZE)
            for _ in range(self.heap_size):
                buf = fh.read(self.rec_data_size)
                if len(buf) < self.rec_data_size:
                    break
                rec = Record.unpack(buf, self.schema)

                # ignorar huecos
                if pk_idx is not None and rec.values[pk_idx] == pk_sentinel:
                    fh.seek(PTR_SIZE, os.SEEK_CUR)
                    continue

                if rec.values[fld_idx] == value:
                    # --- Reemplazar offsets por contenido real para campos 'text' ---
                    updated_values = list(rec.values)
                    for i, (fname, fmt) in enumerate(self.schema):
                        if fmt == "text":
                            offset = updated_values[i]
                            updated_values[i] = TextFile(self.table_name, fname).read(offset)
                    resultados.append(Record(self.schema, updated_values))

                    if stop_early:
                        break

                fh.seek(PTR_SIZE, os.SEEK_CUR)

        # devolver un solo registro si sólo hay uno, si prefieres:
        # return resultados[0] if len(resultados) == 1 else resultados
        return resultados

    # ------------------------------------------------------------------
    # Extracción de índice (ignora huecos) -----------------------------
    # ------------------------------------------------------------------
    def extract_index(self, field):
        names = [n for n, _ in self.schema]
        if field not in names:
            raise KeyError(f"Campo '{field}' no existe.")
        fld_idx = names.index(field)

        pk_idx, pk_sentinel = None, None
        if self.primary_key is not None:
            pk_idx, pk_fmt = self._pk_idx_fmt()
            pk_sentinel = self._sentinel(pk_fmt)

        out, pos = [], 0
        with open(self.filename, "rb") as fh:
            fh.seek(METADATA_SIZE)
            while pos < self.heap_size:
                buf = fh.read(self.rec_data_size)
                if len(buf) < self.rec_data_size:
                    break
                rec = Record.unpack(buf, self.schema)
                # saltar huecos
                if pk_idx is not None and rec.values[pk_idx] == pk_sentinel:
                    fh.seek(PTR_SIZE, os.SEEK_CUR)
                    pos += 1
                    continue
                out.append((rec.values[fld_idx], pos))
                fh.seek(PTR_SIZE, os.SEEK_CUR)
                pos += 1
        return out

    # ------------------------------------------------------------------
    # Fetch por offset --------------------------------------------------
    # ------------------------------------------------------------------
    def fetch_record_by_offset(self, pos: int) -> Record:
        if pos < 0 or pos >= self.heap_size:
            raise IndexError("Offset fuera de rango")
        
        with open(self.filename, "rb") as fh:
            fh.seek(METADATA_SIZE + pos * self.slot_size)
            buf = fh.read(self.rec_data_size)
            record = Record.unpack(buf, self.schema)
            
            # Procesar campos de texto
            updated_values = list(record.values)
            for i, (fname, fmt) in enumerate(self.schema):
                if fmt == "text":
                    offset = updated_values[i]
                    updated_values[i] = TextFile(self.table_name, fname).read(offset)
            
            return Record(self.schema, updated_values)

    # ------------------------------------------------------------------
    # Utilidades de depuración -----------------------------------------
    # ------------------------------------------------------------------
    def print_all(self):
        print(f"heap_size={self.heap_size}, free_head={self.free_head}")
        names = [n for n, _ in self.schema]
        print(" | ".join(names))
        print("-" * 10 * len(names))
        with open(self.filename, "rb") as fh:
            fh.seek(METADATA_SIZE)
            for i in range(self.heap_size):
                buf = fh.read(self.rec_data_size)
                rec = Record.unpack(buf, self.schema)

                # Reemplazar offsets por texto real
                for idx, (name, fmt) in enumerate(self.schema):
                    if fmt == "text":
                        offset = rec.values[idx]
                        text_file = TextFile(self.table_name, name)
                        text_content = text_file.read(offset)
                        rec.values[idx] = text_content

                print(rec)
                fh.seek(PTR_SIZE, os.SEEK_CUR)

    # ------------------------------------------------------------------
    # Utilidades de parser ---------------------------------------------
    # ------------------------------------------------------------------

    def get_all_records(self) -> List[Record]:
        """Devuelve todos los registros no eliminados en una lista."""
        records = []
        with open(self.filename, "rb") as f:
            f.seek(METADATA_SIZE)
            pk_idx, pk_sentinel = None, None

            # get pk for deleted files
            if self.primary_key is not None:
                pk_idx, pk_fmt = self._pk_idx_fmt()
                pk_sentinel = self._sentinel(pk_fmt)

            for _ in range(self.heap_size):
                buf = f.read(self.rec_data_size)
                if len(buf) < self.rec_data_size:
                    break

                rec = Record.unpack(buf, self.schema)

                # skips del records
                if pk_idx is not None and rec.values[pk_idx] == pk_sentinel:
                    f.seek(PTR_SIZE, os.SEEK_CUR)
                    continue
                records.append(rec)
                f.seek(PTR_SIZE, os.SEEK_CUR)
        return records

    @staticmethod
    def to_dataframe(heapfile: "HeapFile", alias=None) -> pd.DataFrame:
        with open(heapfile.filename, "rb") as f:
            f.seek(METADATA_SIZE)
            headers = [
                (heapfile.table_name if alias is None else alias) + "." + column_name
                for column_name, _ in heapfile.schema
            ]
            rows = []

            # get pk for deleted files
            pk_idx, pk_sentinel = None, None
            if heapfile.primary_key is not None:
                pk_idx, pk_fmt = heapfile._pk_idx_fmt()
                pk_sentinel = heapfile._sentinel(pk_fmt)

            for _ in range(heapfile.heap_size):
                buf = f.read(heapfile.rec_data_size)
                if len(buf) < heapfile.rec_data_size:
                    break

                rec = Record.unpack(buf, heapfile.schema)

                # skips del records
                if pk_idx is not None and rec.values[pk_idx] == pk_sentinel:
                    f.seek(PTR_SIZE, os.SEEK_CUR)  # skips ptr
                    continue

                row = {name: value for name, value in zip(headers, rec.values)}
                rows.append(row)

                # skips next_free
                f.seek(PTR_SIZE, os.SEEK_CUR)

            df = pd.DataFrame(rows, columns=headers)
        return df

    # esto es para el spimi, se supone (segun gpt) yield hace que retornes los elementos
    # de una lista de uno en uno, no todo de golpe lo que llenaria la ram
    def iterate_text_documents(self) -> Iterator[Tuple[int, str]]:
        """
        Devuelve (id, texto) de todos los registros válidos,
        concatenando todos los campos 'text' en un solo string.
        """
        text_fields = [i for i, (_, fmt) in enumerate(self.schema) if fmt == "text"]
        pk_idx, _ = self._pk_idx_fmt()
        sentinel = self._sentinel(self.schema[pk_idx][1])

        with open(self.filename, "rb") as fh:
            fh.seek(METADATA_SIZE)
            for i in range(self.heap_size):
                buf = fh.read(self.rec_data_size)
                if len(buf) < self.rec_data_size:
                    break
                rec = Record.unpack(buf, self.schema)
                if rec.values[pk_idx] == sentinel:
                    fh.seek(PTR_SIZE, os.SEEK_CUR)
                    continue
                text = " ".join(
                    TextFile(self.table_name, self.schema[idx][0]).read(offset)
                    for idx in text_fields
                    for offset in [rec.values[idx]]
                )
                fh.seek(PTR_SIZE, os.SEEK_CUR)
                yield rec.values[pk_idx], text
