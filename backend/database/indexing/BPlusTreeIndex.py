from .IndexRecord import IndexRecord
from . import utils
import struct
import os
import math
import json

NODE_HEADER_FORMAT = 'iiQ'  # is_leaf, key_count, next_leaf_offset

class BPlusTreeNode:
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf

        if is_leaf:
            self.records = []      # List[IndexRecord] en hojas
            self.next = None       
        else:
            self.keys = []         
            self.children = []     

class BPlusTreeIndex:
    def __init__(self, order, filename, auxname, index_format='i'):
        self.order = order
        self.min_keys = self.order // 2
        self.max_keys = order
        self.filename = filename
        self.auxname = auxname
        self.index_format = index_format

        self.key_size = struct.calcsize(self.index_format)
        self.node_size_internal = 16 + self.key_size * self.max_keys + 8 * (self.max_keys + 1)

        default_key = utils.get_default_key(index_format)
        sample_record = IndexRecord(index_format, default_key, 0)
        self.leaf_record_size = sample_record.size
        self.node_size_leaf = 16 + self.order * self.leaf_record_size

        try:
            with open(self.auxname, 'rb') as f:
                self.root_offset = struct.unpack('Q', f.read(8))[0]
        except (FileNotFoundError, struct.error):
            root = BPlusTreeNode(is_leaf=True)

            with open(self.auxname, 'wb') as f:
                f.write(struct.pack('Q', 0))

            self.root_offset = self.save_node(root)
            self.update_root_offset(self.root_offset)

    def load_node(self, node_offset):
        with open(self.auxname, 'rb') as f:
            f.seek(node_offset)
            header = f.read(16)
            if len(header) < 16:
                raise ValueError("No se pudo leer el encabezado del nodo.")
            is_leaf, key_count, next_leaf = struct.unpack('iiQ', header)

            node_size = self.node_size_leaf if is_leaf else self.node_size_internal
            f.seek(node_offset)
            buffer = f.read(node_size)

        is_leaf, key_count, next_leaf = struct.unpack('iiQ', buffer[:16])
        node = BPlusTreeNode(is_leaf=bool(is_leaf))

        if is_leaf:
            node.next = next_leaf
            node.records = []
            record_size = self.leaf_record_size
            for i in range(key_count):
                start = 16 + i * record_size
                end = start + record_size
                record_data = buffer[start:end]
                if len(record_data) < record_size:
                    raise ValueError(f"Registro incompleto: se esperaban {record_size} bytes, pero se leyeron {len(record_data)}.")
                record = IndexRecord.unpack(record_data, self.index_format)
                node.records.append(record)
        else:
            keys_start = 16
            keys = []
            for i in range(self.max_keys):
                if self.index_format == 'i':
                    k = struct.unpack_from('i', buffer, keys_start + i * 4)[0]
                elif self.index_format == 'f':
                    k = struct.unpack_from('f', buffer, keys_start + i * 4)[0]
                elif 's' in self.index_format:
                    size = self.key_size
                    raw = struct.unpack_from(f'{size}s', buffer, keys_start + i * size)[0]
                    k = raw.rstrip(b'\x00').decode()
                else:
                    raise ValueError("Formato de clave no soportado")
                keys.append(k)

            children_start = keys_start + self.key_size * self.max_keys
            children_data = buffer[children_start:children_start + 8 * (self.max_keys + 1)]
            if len(children_data) < 8 * (self.max_keys + 1):
                raise ValueError("No se pudo leer todos los hijos del nodo.")
            children = list(struct.unpack(f'{self.max_keys + 1}Q', children_data))

            node.keys = keys[:key_count]
            node.children = children[:key_count + 1]

        return node

    def save_node(self, node):
        is_leaf = int(node.is_leaf)
        key_count = len(node.records if node.is_leaf else node.keys)
        next_leaf = node.next if (node.is_leaf and node.next is not None) else 0
        header = struct.pack('iiQ', is_leaf, key_count, next_leaf)

        if node.is_leaf:
            records_bytes = b''.join(record.pack() for record in node.records)
            padding = b'\x00' * (self.node_size_leaf - 16 - len(records_bytes))
            buffer = header + records_bytes + padding
        else:
            if 's' in self.index_format:
                pad_key = b'\x00' * self.key_size
            else:
                pad_key = 0
            keys = node.keys + [pad_key] * (self.max_keys - len(node.keys))
            children = node.children + [0] * (self.max_keys + 1 - len(node.children))

            key_data = b''
            for k in keys:
                if self.index_format == 'i':
                    key_data += struct.pack('i', k)
                elif self.index_format == 'f':
                    key_data += struct.pack('f', k)
                elif 's' in self.index_format:
                    encoded = str(k).encode('utf-8')[:self.key_size].ljust(self.key_size, b'\x00')
                    key_data += struct.pack(f'{self.key_size}s', encoded)
                else:
                    raise ValueError("Formato de clave no soportado")

            child_data = struct.pack(f'{self.max_keys + 1}Q', *children)
            buffer = header + key_data + child_data
            padding = b'\x00' * (self.node_size_internal - len(buffer))
            buffer += padding

        with open(self.auxname, 'ab') as f:
            pos = f.tell()
            f.write(buffer)
            return pos

    def save_node_at(self, offset, node):
        is_leaf = int(node.is_leaf)
        key_count = len(node.records if node.is_leaf else node.keys)
        next_leaf = node.next if (node.is_leaf and node.next is not None) else 0
        header = struct.pack('iiQ', is_leaf, key_count, next_leaf)

        if node.is_leaf:
            records_bytes = b''.join(record.pack() for record in node.records)
            padding = b'\x00' * (self.node_size_leaf - 16 - len(records_bytes))
            buffer = header + records_bytes + padding
        else:
            if 's' in self.index_format:
                pad_key = b'\x00' * self.key_size
            else:
                pad_key = 0
            keys = node.keys + [pad_key] * (self.max_keys - len(node.keys))
            children = node.children + [0] * (self.max_keys + 1 - len(node.children))

            key_data = b''
            for k in keys:
                if self.index_format == 'i':
                    key_data += struct.pack('i', k)
                elif self.index_format == 'f':
                    key_data += struct.pack('f', k)
                elif 's' in self.index_format:
                    encoded = str(k).encode('utf-8')[:self.key_size].ljust(self.key_size, b'\x00')
                    key_data += struct.pack(f'{self.key_size}s', encoded)
                else:
                    raise ValueError("Formato de clave no soportado")

            child_data = struct.pack(f'{self.max_keys + 1}Q', *children)
            buffer = header + key_data + child_data
            padding = b'\x00' * (self.node_size_internal - len(buffer))
            buffer += padding

        with open(self.auxname, 'r+b') as f:
            f.seek(offset)
            f.write(buffer)
        
    def update_root_offset(self, offset):
        with open(self.auxname, 'r+b') as f:
            f.seek(0)
            f.write(struct.pack('Q', offset))
        self.root_offset = offset

    def insert(self, record):
        #print(f"[DEBUG INSERT] Insertando clave: {record.key}, offset: {record.offset}")

        # No escribas al archivo ni calcules un nuevo offset
        index_record = IndexRecord(self.index_format, record.key, record.offset)
        result = self._insert_aux(self.root_offset, index_record)

        if result:
            new_node_offset, separator_key = result
            new_root = BPlusTreeNode(is_leaf=False)
            new_root.keys = [separator_key]
            new_root.children = [self.root_offset, new_node_offset]
            new_root_offset = self.save_node(new_root)
            self.update_root_offset(new_root_offset)

    def _insert_aux(self, node_offset, index_record):
        node = self.load_node(node_offset)

        if node.is_leaf:
            #print(f"[DEBUG _insert_aux] Insertando en hoja. Clave: {index_record.key}")        
            idx = 0
            while idx < len(node.records) and index_record.key > node.records[idx].key:
                idx += 1
            node.records.insert(idx, index_record)

            if len(node.records) > self.order:
                return self._split_leaf(node, node_offset)
            else:
                self.save_node_at(node_offset, node)
                return None

        else:
            #print(f"[DEBUG _insert_aux] Descendiendo en nodo interno. Clave: {index_record.key}")
            idx = 0
            while idx < len(node.keys) and index_record.key > node.keys[idx]:
                idx += 1
            result = self._insert_aux(node.children[idx], index_record)

            if result:
                #print(f"[DEBUG _insert_aux] Split recibido desde hijo. Clave: {index_record.key}")
                new_node_offset, new_key = result
                node.keys.insert(idx, new_key)
                node.children.insert(idx + 1, new_node_offset)

                if len(node.keys) > self.order:
                    return self._split_internal(node, node_offset)
                else:
                    self.save_node_at(node_offset, node)
                    return None
            else:
                return None

    def _split_leaf(self, node, node_offset):
        mid = len(node.records) // 2

        new_leaf = BPlusTreeNode(is_leaf=True)
        new_leaf.records = node.records[mid:]
        new_leaf.next = node.next

        node.records = node.records[:mid]
        node.next = self.save_node(new_leaf)

        new_leaf_offset = node.next
        self.save_node_at(node_offset, node)

        return new_leaf_offset, new_leaf.records[0].key

    def _split_internal(self, node, node_offset):
        mid = len(node.keys) // 2

        new_internal = BPlusTreeNode(is_leaf=False)
        new_internal.keys = node.keys[mid + 1:]
        new_internal.children = node.children[mid + 1:]

        separator_key = node.keys[mid]

        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]

        new_offset = self.save_node(new_internal)
        self.save_node_at(node_offset, node)

        return new_offset, separator_key

    def search(self, key):
        #print(f"[DEBUG SEARCH] Buscando clave: {key}")    
        return self.search_aux(self.root_offset, key)

    def search_aux(self, node_offset, key):
        node = self.load_node(node_offset)

        if node.is_leaf:
            matches = []
            for record in node.records:
                if record.key == key:
                    matches.append(record.offset)
                elif record.key > key:
                    break
            return matches

        else:
            idx = 0
            while idx < len(node.keys) and key >= node.keys[idx]:
                idx += 1
            return self.search_aux(node.children[idx], key)

    def range_search(self, min_key, max_key):
        offsets = []
        self.range_search_aux(self.root_offset, min_key, max_key, offsets)

        return offsets if offsets else None

    def range_search_aux(self, node_offset, min_key, max_key, result_list):
        node = self.load_node(node_offset)

        if node.is_leaf:
            while node is not None:
                for record in node.records:
                    if min_key <= record.key <= max_key:
                        result_list.append(record.offset)
                    elif record.key > max_key:
                        return
                if node.next:
                    node = self.load_node(node.next)
                else:
                    break
        else:
            idx = 0
            while idx < len(node.keys) and node.keys[idx] < min_key:
                idx += 1
            self.range_search_aux(node.children[idx], min_key, max_key, result_list)

    def _handle_leaf_underflow(self, node, node_offset):
        """
        Intenta redistribuir con un hermano. Si no es posible, fusión (caso siguiente).
        """
        parent_offset, parent_node, idx_in_parent = self._find_parent(self.root_offset, node_offset)

        # Verifica hermanos
        left_sibling = None
        right_sibling = None
        left_offset = None
        right_offset = None

        if idx_in_parent > 0:
            left_offset = parent_node.children[idx_in_parent - 1]
            left_sibling = self.load_node(left_offset)

        if idx_in_parent < len(parent_node.children) - 1:
            right_offset = parent_node.children[idx_in_parent + 1]
            right_sibling = self.load_node(right_offset)

        # Intenta redistribuir con el hermano izquierdo
        if left_sibling and len(left_sibling.records) > self.min_keys:
            borrowed = left_sibling.records.pop()
            node.records.insert(0, borrowed)
            parent_node.keys[idx_in_parent - 1] = node.records[0].key

            self.save_node_at(left_offset, left_sibling)
            self.save_node_at(node_offset, node)
            self.save_node_at(parent_offset, parent_node)
            return None

        # Intenta redistribuir con el hermano derecho
        if right_sibling and len(right_sibling.records) > self.min_keys:
            borrowed = right_sibling.records.pop(0)
            node.records.append(borrowed)
            parent_node.keys[idx_in_parent] = right_sibling.records[0].key

            self.save_node_at(right_offset, right_sibling)
            self.save_node_at(node_offset, node)
            self.save_node_at(parent_offset, parent_node)
            return None

        return None
    
    def _handle_internal_underflow(self, node_offset):
        """
        Maneja el underflow en nodos internos: redistribución o fusión.
        """
        parent_offset, parent_node, idx_in_parent = self._find_parent(self.root_offset, node_offset)
        node = self.load_node(node_offset)

        # Verificar hermanos
        left_sibling = None
        right_sibling = None
        left_offset = None
        right_offset = None

        if idx_in_parent > 0:
            left_offset = parent_node.children[idx_in_parent - 1]
            left_sibling = self.load_node(left_offset)

        if idx_in_parent < len(parent_node.children) - 1:
            right_offset = parent_node.children[idx_in_parent + 1]
            right_sibling = self.load_node(right_offset)

        # ----- Redistribuir desde el hermano izquierdo -----
        if left_sibling and len(left_sibling.keys) > self.min_keys:
            sep_key = parent_node.keys[idx_in_parent - 1]

            # Toma el último hijo del izquierdo
            borrowed_key = left_sibling.keys.pop()
            borrowed_child = left_sibling.children.pop()

            node.keys.insert(0, sep_key)
            node.children.insert(0, borrowed_child)
            parent_node.keys[idx_in_parent - 1] = borrowed_key

            self.save_node_at(left_offset, left_sibling)
            self.save_node_at(node_offset, node)
            self.save_node_at(parent_offset, parent_node)
            return

        # ----- Redistribuir desde el hermano derecho -----
        if right_sibling and len(right_sibling.keys) > self.min_keys:
            sep_key = parent_node.keys[idx_in_parent]

            borrowed_key = right_sibling.keys.pop(0)
            borrowed_child = right_sibling.children.pop(0)

            node.keys.append(sep_key)
            node.children.append(borrowed_child)
            parent_node.keys[idx_in_parent] = borrowed_key

            self.save_node_at(right_offset, right_sibling)
            self.save_node_at(node_offset, node)
            self.save_node_at(parent_offset, parent_node)
            return

        # ----- Fusión con hermano izquierdo -----
        if left_sibling:
            sep_key = parent_node.keys.pop(idx_in_parent - 1)
            parent_node.children.pop(idx_in_parent)

            left_sibling.keys.append(sep_key)
            left_sibling.keys += node.keys
            left_sibling.children += node.children

            self.save_node_at(left_offset, left_sibling)
            self.save_node_at(parent_offset, parent_node)
            return

        # ----- Fusión con hermano derecho -----
        if right_sibling:
            sep_key = parent_node.keys.pop(idx_in_parent)
            parent_node.children.pop(idx_in_parent + 1)

            node.keys.append(sep_key)
            node.keys += right_sibling.keys
            node.children += right_sibling.children

            self.save_node_at(node_offset, node)
            self.save_node_at(parent_offset, parent_node)
            return

        # Si el padre se queda sin claves, actualizar raíz
        if parent_node == self.load_node(self.root_offset) and len(parent_node.keys) == 0:
            new_root_offset = parent_node.children[0]
            self.update_root_offset(new_root_offset)
    
    def _find_parent(self, current_offset, child_offset):
        current_node = self.load_node(current_offset)

        if current_node.is_leaf:
            return None, None, None  # Las hojas no tienen hijos

        for i, ptr in enumerate(current_node.children):
            if ptr == child_offset:
                return current_offset, current_node, i
            else:
                result = self._find_parent(ptr, child_offset)
                if result[0] is not None:
                    return result
        return None, None, None

    def delete(self, key, offset):
        #print(f"[DEBUG DELETE] Eliminando clave: {key}, offset: {offset}")
        self._delete_aux(self.root_offset, key, offset)

    def _delete_aux(self, node_offset, key, offset):
        node = self.load_node(node_offset)

        if node.is_leaf:
            print(f"[DEBUG DELETE] Nodo hoja actual: {[r.key for r in node.records]}")
            for i, rec in enumerate(node.records):
                print(f"[DEBUG] Comparando con record: {rec.key=} {rec.offset=} vs objetivo: {key=} {offset=}")
                if rec.key == key and rec.offset == offset:
                    del node.records[i]
                    self.save_node_at(node_offset, node)
                    print(f"[DEBUG DELETE] Eliminado en hoja. Claves ahora: {[r.key for r in node.records]}")
                    return True
            print(f"[DEBUG DELETE] Clave no encontrada en esta hoja.")
            return False

        # Nodo interno: buscar el hijo correspondiente
        idx = 0
        while idx < len(node.keys) and key > node.keys[idx]:
            idx += 1
        self._delete_aux(node.children[idx], key, offset)

        # Si el hijo quedó en underflow, manejamos el caso
        child_node = self.load_node(node.children[idx])
        if not child_node.is_leaf and len(child_node.keys) < self.min_keys:
            self._handle_internal_underflow(node.children[idx])

    def scan_all(self):
        node = self.load_node(self.root_offset)
        # Bajar hasta la hoja más a la izquierda
        while not node.is_leaf:
            node = self.load_node(node.children[0])
        
        print("\n[SCAN ALL] Registros en las hojas del árbol:")
        while node:
            for rec in node.records:
                print(f"  {rec.key!r} → {rec.offset}")
            node = self.load_node(node.next) if node.next else None

    @staticmethod
    def build_index(table_path: str, extract_index_fn, key_field: str, order: int = 4):
        """
        Crea un archivo de índice B+ Tree desde una tabla existente.

        Args:
            table_path: Ruta base de la tabla (sin extensión)
            extract_index_fn: Función que devuelve [(key, offset)]
            key_field: Campo sobre el cual se indexará
            order: Orden del árbol B+ (máx claves por nodo)
        """
        schema_path = f"{table_path}.schema.json"
        with open(schema_path, "r") as f:
            schema = json.load(f)

        field_format = None
        for field in schema["fields"]:
            if field["name"] == key_field:
                field_format = field["type"]
                break
        if field_format is None:
            raise ValueError(f"Campo '{key_field}' no encontrado en el esquema.")

        btree = BPlusTreeIndex(
            order=order,
            filename=table_path + ".dat",
            auxname=f"{table_path}.{key_field}.btree.idx",
            index_format=field_format
        )

        entries = extract_index_fn(key_field)
        entries.sort(key=lambda x: x[0])

        #print("\n[DEBUG] Entradas extraídas para el índice B+ Tree:")
        #for key, offset in entries[:10]:
        #    print(f"  Key: {key!r} -> Offset: {offset}")

        for key, offset in entries:
            index_record = IndexRecord(field_format, key, offset)
            btree.insert(index_record)

        return True

#---------------------------------------------------------------------------------------------------------

class BPlusTreeIndexWrapper:
    def __init__(self, table_path: str, field_name: str):
        self.table_path = table_path
        self.field_name = field_name

        schema_file = f"{table_path}.schema.json"
        with open(schema_file, "r") as f:
            schema = json.load(f)
        for field in schema["fields"]:
            if field["name"] == field_name:
                self.index_format = field["type"]
                break
        else:
            raise ValueError(f"Campo '{field_name}' no encontrado en el esquema.")

        self.tree = BPlusTreeIndex(
            order=4,
            filename=table_path + ".dat",
            auxname=f"{table_path}.{field_name}.btree.idx",
            index_format=self.index_format
        )

    def insert_record(self, index_record: IndexRecord):
        self.tree.insert(index_record)

    def search(self, key):
        return self.tree.search(key)

    def range_search(self, min_key, max_key):
        return self.tree.range_search(min_key, max_key)

    def delete_record(self, key, offset):
        return self.tree.delete(key, offset)

    def print_all(self):
        return self.tree.scan_all()