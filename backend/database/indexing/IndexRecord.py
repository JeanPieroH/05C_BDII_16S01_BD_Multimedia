import struct
from typing import Union, Tuple, Any
import math
import re

# Regex for string and tuple
re_string = re.compile(r"^(\d+)s$")
re_tuple = re.compile(r"^(\d+)([fdi])$")

class IndexRecord:
    """Registro de índice que soporta claves de tipo int, float o string."""
    
    # Tipos soportados
    TYPE_INT = 0
    TYPE_FLOAT = 1
    TYPE_STRING = 2
    TYPE_TUPLE = 3
    
    def __init__(self, format: str, key: Union[int, float, str, Tuple[Union[int, float], ...]], offset: int):
        """
        Inicializa el registro de índice.
        
        Args:
            format: Formato del campo ('i', 'f', o 'Ns' donde N es tamaño string)
            key: Valor de la clave
            offset: Posición en el archivo de datos
        """
        self.format = format
        self.key = key
        self.offset = offset
        self._validate_types()
        
    def _validate_types(self):
        if self.format == 'i':
            if not isinstance(self.key, int):
                raise TypeError(f"Clave debe ser int para formato 'i', se recibió {type(self.key)}")

        elif self.format == 'f':
            if not isinstance(self.key, float):
                raise TypeError(f"Clave debe ser float para formato 'f', se recibió {type(self.key)}")

        elif re_string.fullmatch(self.format):
            n = int(self.format[:-1])
            if not isinstance(self.key, str):
                raise TypeError(f"Clave debe ser str para formato string, se recibió {type(self.key)}")
            raw = self.key.encode("utf-8")
            if len(raw) > n:
                raise ValueError(f"Cadena demasiado larga: {len(raw)} bytes, máximo {n}")

        elif re_tuple.fullmatch(self.format):
            n = int(self.format[:-1])
            type_char = self.format[-1]
            if not isinstance(self.key, tuple) or len(self.key) != n:
                raise TypeError(f"Clave debe ser tupla de tamaño {n} para el formato {self.format}")
            
            if type_char == 'i' and not all(isinstance(x, int) for x in self.key):
                raise TypeError(f"Todos los elementos deben ser int para formato {self.format}")
            elif type_char in ('f', 'd') and not all(isinstance(x, float) for x in self.key):
                raise TypeError(f"Todos los elementos deben ser float para formato {self.format}")

        else:
            raise TypeError(f"Formato no soportado: {self.format}")

    
    def pack(self) -> bytes:
        """Serializa el registro a bytes."""
        if self.format == 'i':
            return struct.pack("<Bii", self.TYPE_INT, self.key, self.offset)
        
        if self.format == 'f':
            return struct.pack("<Bfi", self.TYPE_FLOAT, self.key, self.offset)
        
        if re_string.fullmatch(self.format) and isinstance(self.key, str):
            n = int(self.format[:-1])
            raw = self.key.encode("utf-8")[:n].ljust(n, b'\x00')
            return struct.pack(f"<B{n}si", self.TYPE_STRING, raw, self.offset)
        
        if re_tuple.fullmatch(self.format) and isinstance(self.key, tuple):
            n = int(self.format[:-1])
            type_char = self.format[-1]
            return struct.pack(f"<B{n}{type_char}i", self.TYPE_TUPLE, *self.key, self.offset)
        
        else:
            raise ValueError(f"Formato no soportado: '{self.format}', con tipo: '{type(self.key)}'")

    @staticmethod
    def unpack(data: bytes, format: str) -> 'IndexRecord':
        """Deserializa un registro desde bytes."""
        type_byte = data[0]
        
        if type_byte == IndexRecord.TYPE_INT:
            _, key, offset = struct.unpack_from("<Bii", data)
            return IndexRecord('i', key, offset)
        
        if type_byte == IndexRecord.TYPE_FLOAT:
            _, key, offset = struct.unpack_from("<Bfi", data)
            return IndexRecord('f', key, offset)
        
        if type_byte == IndexRecord.TYPE_STRING:
            n = int(format[:-1])
            _, raw, offset = struct.unpack_from(f"<B{n}si", data)
            key = raw.decode('utf-8').rstrip('\x00')
            return IndexRecord(format, key, offset)
        
        if type_byte == IndexRecord.TYPE_TUPLE:
            n = int(format[:-1])
            type_char = format[-1]
            parts = struct.unpack_from(f"<B{n}{type_char}i", data)
            key = tuple(parts[1:n+1])
            offset = parts[-1]
            return IndexRecord(format, key, offset)

        else:
            raise ValueError(f"Tipo de registro desconocido: {type_byte}")

    @property
    def size(self) -> int:
        """Tamaño en bytes del registro serializado."""
        if self.format == 'i':
            return struct.calcsize("<Bii")
        
        if self.format == 'f':
            return struct.calcsize("<Bfi")
        
        if re_string.fullmatch(self.format):
            n = int(self.format[:-1])
            return struct.calcsize(f"<B{n}si")
        
        if re_tuple.fullmatch(self.format):
            n = int(self.format[:-1])
            type_char = self.format[-1]
            return struct.calcsize(f"<B{n}{type_char}i")
        
        else:
            raise ValueError(f"Formato no soportado para size(): {self.format}")
    
    def __eq__(self, other: 'IndexRecord') -> bool:
        """Comparación por igualdad."""
        if not isinstance(other, IndexRecord):
            raise TypeError("Comparación solo válida entre IndexRecords")

        return self.key == other.key and self.offset == other.offset
    
    def __lt__(self, other: 'IndexRecord') -> bool:
        """Comparación menor que para ordenamiento."""
        if not isinstance(other, IndexRecord):
            raise TypeError("Comparación solo válida entre IndexRecords")
        
        if self.format != other.format:
            raise TypeError(f"No se puede comparar registros con campos de formatos distintos: '{self.format}' vs '{other.format}'")
        
        try:
            return self.key < other.key  # type: ignore
        
        except TypeError:
            raise TypeError(f"Claves no comparables: {self.key!r} vs {other.key!r}")
    
    def __repr__(self) -> str:
        return f"IndexRecord(format='{self.format}', key={self.key}, offset={self.offset})"