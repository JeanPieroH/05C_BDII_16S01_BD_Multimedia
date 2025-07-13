# indexing/ExtendibleHashIndex.py

"""
Implementación de un Índice Hash Extensible en disco, compatible con database.py.

Estructura de archivos:
- {table}.{field}.hash.idx   → Archivo marcador (para database.py)
- {table}.{field}.hash.db    → Buckets binarios fijos
- {table}.{field}.hash.tree  → Árbol binario serializado con pickle
"""

from __future__ import annotations
import os, struct, pickle, hashlib
from typing import List, Union
from .IndexRecord import IndexRecord
from . import utils  # utils para schema y formatos

# --------------------
# Configuraciones globales
# --------------------
GLOBAL_DEPTH  = 16          # Profundidad máxima del trie (2^16 posibilidades)
BUCKET_FACTOR = 4           # Cantidad máxima de registros por bucket
SENTINEL_INT  = -1
SENTINEL_STR  = ""

# --------------------
# Registro Interno _Rec (clave, offset)
# --------------------
class _Rec:
    __slots__ = ("key", "offset")
    def __init__(self, key, offset):
        self.key, self.offset = key, offset

    def to_bytes(self):
        return pickle.dumps((self.key, self.offset))

    @classmethod
    def from_bytes(cls, blob):
        return cls(*pickle.loads(blob))


# --------------------
# Página de bucket con encadenamiento de overflow
# --------------------
class _Page:
    FORMAT = "!ii8s"  # num_records, next_page, padding
    SIZE = struct.calcsize(FORMAT)

    def __init__(self, pid: int, cap: int, store: _Storage):
        self.pid = pid
        self.cap = cap
        self.store = store
        self.next = -1
        self.data: List[_Rec] = []

    def _read(self):
        return self.store._read(self.pid)

    def _write(self, blob):
        self.store._write(self.pid, blob)

    def load(self):
        raw = self._read()
        n, nxt, _ = struct.unpack(self.FORMAT, raw[:self.SIZE])
        self.next = nxt
        self.data.clear()
        p = self.SIZE
        for _ in range(n):
            l = struct.unpack("!I", raw[p:p+4])[0]
            p += 4
            item = raw[p:p+l]
            p += l
            self.data.append(_Rec.from_bytes(item))

    def save(self):
        body = b"".join(struct.pack("!I", len(r.to_bytes())) + r.to_bytes() for r in self.data)
        head = struct.pack(self.FORMAT, len(self.data), self.next, b"\x00"*8)
        self._write(head + body)

    def is_full(self):
        return len(self.data) >= self.cap

    def insert(self, rec: _Rec) -> bool:
        self.load()
        if not self.is_full():
            self.data.append(rec)
            self.save()
            return True
        if self.next != -1:
            return self.store.page(self.next).insert(rec)
        return False

    def search(self, key) -> List[_Rec]:
        self.load()
        matches = [r for r in self.data if r.key == key]
        if self.next != -1:
            matches += self.store.page(self.next).search(key)
        return matches


    def delete(self, key):
        self.load()
        for i, r in enumerate(self.data):
            if r.key == key:
                del self.data[i]
                self.save()
                return True
        if self.next == -1:
            return False
        deleted = self.store.page(self.next).delete(key)
        if deleted and not self.store.page(self.next)._has_records():
            overflow = self.store.page(self.next)
            self.next = overflow.next
            self.save()
        return deleted

    def get_all(self):
        self.load()
        output = list(self.data)
        if self.next != -1:
            output.extend(self.store.page(self.next).get_all())
        return output

    def _has_records(self):
        self.load()
        return bool(self.data) or self.next != -1


# --------------------
# Manejador de almacenamiento binario
# --------------------
class _Storage:
    HEADER_FORMAT = "!ii8s"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, filename: str, capacity: int):
        self.filename = filename
        self.cap = capacity
        if not os.path.exists(filename):
            with open(filename, "wb") as f:
                f.write(struct.pack(self.HEADER_FORMAT, 0, self.cap, b"\x00"*8))
            self.next_pid = 0
        else:
            with open(filename, "rb") as f:
                self.next_pid, self.cap, _ = struct.unpack(self.HEADER_FORMAT, f.read(self.HEADER_SIZE))

    def _page_size(self):
        avg = 128
        return _Page.SIZE + self.cap * (4 + avg)

    def _pos(self, pid):
        return self.HEADER_SIZE + pid * self._page_size()

    def _read(self, pid):
        with open(self.filename, "rb") as f:
            f.seek(self._pos(pid))
            return f.read(self._page_size())

    def _write(self, pid, blob):
        blob = blob.ljust(self._page_size(), b"\x00")
        with open(self.filename, "r+b") as f:
            f.seek(self._pos(pid))
            f.write(blob)

    def _write_header(self):
        with open(self.filename, "r+b") as f:
            f.seek(0)
            f.write(struct.pack(self.HEADER_FORMAT, self.next_pid, self.cap, b"\x00"*8))

    def new_page(self):
        pid = self.next_pid
        self.next_pid += 1
        self._write_header()
        page = _Page(pid, self.cap, self)
        page.save()
        return page

    def page(self, pid):
        return _Page(pid, self.cap, self)

# ========================
# Nodo binario del trie
# ========================
class _Node:
    __slots__ = ("level", "left", "right", "pid")
    def __init__(self, level: int, pid: int):
        self.level = level
        self.left = self.right = None
        self.pid = pid  # Página asignada si es hoja

    def is_leaf(self) -> bool:
        return self.left is None and self.right is None


# ==========================
# Árbol hash extensible
# ==========================
class _HashTree:
    def __init__(self, base_path: str, key_format: str):
        self.base = base_path
        self.db_path = f"{base_path}.hash.db"
        self.tree_path = f"{base_path}.hash.tree"
        self.kfmt = key_format
        self.store = _Storage(self.db_path, BUCKET_FACTOR)

        if os.path.exists(self.tree_path):
            with open(self.tree_path, "rb") as f:
                self.root = pickle.load(f)
        else:
            # Árbol inicial con 2 páginas
            p0 = self.store.new_page()
            p1 = self.store.new_page()
            self.root = _Node(0, None)
            self.root.left = _Node(1, p0.pid)
            self.root.right = _Node(1, p1.pid)
            self._save()

    def _save(self):
        with open(self.tree_path, "wb") as f:
            pickle.dump(self.root, f)

    def _hash_bits(self, key: Union[int, str]) -> str:
        if isinstance(key, str):
            h = int.from_bytes(hashlib.sha256(key.encode()).digest(), "little")
        else:
            h = key & 0xFFFFFFFF
        return format(h, f"0{GLOBAL_DEPTH}b")

    def _leaf(self, bits: str) -> _Node:
        node = self.root
        while not node.is_leaf():
            node = node.left if bits[node.level] == "0" else node.right
        return node

    def insert(self, key: Union[int, str], offset: int):
        bits = self._hash_bits(key)
        leaf = self._leaf(bits)
        page = self.store.page(leaf.pid)
        if page.insert(_Rec(key, offset)):
            return
        if leaf.level >= GLOBAL_DEPTH - 1:
            # No se puede dividir más: chaining
            new_page = self.store.new_page()
            page.load()
            page.next = new_page.pid
            page.save()
            new_page.insert(_Rec(key, offset))
            return
        self._split(leaf, _Rec(key, offset))

    def _split(self, leaf: _Node, extra_rec: _Rec = None):
        page = self.store.page(leaf.pid)
        items = page.get_all()
        if extra_rec:
            items.append(extra_rec)

        left_page = self.store.new_page()
        right_page = self.store.new_page()

        leaf.left = _Node(leaf.level + 1, left_page.pid)
        leaf.right = _Node(leaf.level + 1, right_page.pid)
        leaf.pid = None

        for r in items:
            bit = self._hash_bits(r.key)[leaf.level]
            target = leaf.left if bit == "0" else leaf.right
            self.store.page(target.pid).insert(r)

        self._save()

    def search(self, key: Union[int, str]) -> List[int]:
        bits = self._hash_bits(key)
        page = self.store.page(self._leaf(bits).pid)
        results = page.search(key)
        return [r.offset for r in results] if results else []


    def delete(self, key: Union[int, str]):
        bits = self._hash_bits(key)
        page = self.store.page(self._leaf(bits).pid)
        page.delete(key)
        self._save()

    def all_records(self) -> List[_Rec]:
        result = []
        def dfs(node: _Node):
            if node.is_leaf():
                result.extend(self.store.page(node.pid).get_all())
            else:
                dfs(node.left)
                dfs(node.right)
        dfs(self.root)
        return result

class ExtendibleHashIndex:
    def __init__(self, table_path: str, field_name: str):
        self.idx_file = f"{table_path}.{field_name}.hash.idx"
        if not os.path.exists(self.idx_file):
            raise FileNotFoundError(f"Índice hash no encontrado: {self.idx_file}")
        schema = utils.load_schema(table_path)
        self.kfmt = utils.get_key_format_from_schema(schema, field_name)
        self.tree = _HashTree(f"{table_path}.{field_name}", self.kfmt)

    def search_record(self, key: Union[int, str]) -> List[IndexRecord]:
        self._check_type(key)
        offsets = self.tree.search(key)
        return [IndexRecord(self.kfmt, key, off) for off in offsets]

    def insert_record(self, idx_rec: IndexRecord):
        if idx_rec.format != self.kfmt:
            raise TypeError("Formato de clave no coincide.")
        if self._is_sentinel(idx_rec.key):
            return
        self.tree.insert(idx_rec.key, idx_rec.offset)

    def delete_record(self, key: Union[int, str], offset: int) -> bool:
        self._check_type(key)
        self.tree.delete(key)
        return True

    def print_all(self):
        for r in sorted(self.tree.all_records(), key=lambda x: x.key):
            print(f"{r.key!r} -> {r.offset}")

    @staticmethod
    def build_index(table_path: str, extract_fn, field_name: str) -> bool:
        schema = utils.load_schema(table_path)
        kfmt = utils.get_key_format_from_schema(schema, field_name)
        tree = _HashTree(f"{table_path}.{field_name}", kfmt)
        for key, offset in extract_fn(field_name):
            if key == (SENTINEL_INT if kfmt == "i" else SENTINEL_STR):
                continue
            tree.insert(key, offset)
        tree._save()
        open(f"{table_path}.{field_name}.hash.idx", "wb").close()
        return True

    def _check_type(self, key: Union[int, str]):
        if (self.kfmt == "i" and not isinstance(key, int)) or ("s" in self.kfmt and not isinstance(key, str)):
            raise TypeError("Tipo de clave no coincide con el índice.")

    def _is_sentinel(self, key: Union[int, str]) -> bool:
        return key == (SENTINEL_INT if self.kfmt == "i" else SENTINEL_STR)
