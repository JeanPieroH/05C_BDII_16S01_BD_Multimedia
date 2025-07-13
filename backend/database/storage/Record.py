import struct

# Esta clase Record representa un registro binario genérico.
# A diferencia de versiones más rígidas (como las que usan FORMAT fijo),
# esta clase permite definir cualquier combinación de campos y tipos
# mediante un esquema dinámico que se pasa como parámetro.
# Esto permite reutilizar la misma clase para diferentes tablas
# sin necesidad de modificar su definición interna.

# La idea para que esto funcione es darle el esquema al crear cada registro:
# schema = [("id", "i"), ("nombre", "20s"), ("precio", "f"), ("cantidad", "i")]
# values = [3, "Caramelos", 1.75, 25]
# registro = Record(schema, values)

class Record:

    def __init__(self, schema, values):
        self.schema = schema
        self.values = values
        self.format = ''.join('i' if fmt == 'text' else fmt for _, fmt in schema)
        self.size = struct.calcsize(self.format)

    def pack(self) -> bytes:
        processed = []
        for (_, fmt), val in zip(self.schema, self.values):
            if 's' in fmt:                         # cadena fija
                size = int(fmt[:-1])
                processed.append(val.encode()[:size].ljust(size, b'\x00'))
            elif fmt[:-1].isdigit():               # 3i, 4f, etc.
                if not (isinstance(val, (list, tuple)) and len(val) == int(fmt[:-1])):
                    raise ValueError(f"Se esperaban {fmt[:-1]} elementos para '{fmt}'")
                processed.extend(val)              # aplanar
            else:
                processed.append(val)
        return struct.pack(self.format, *processed)

    @staticmethod
    def unpack(buf, schema):
        fmt_str = ''.join('i' if fmt == 'text' else fmt for _, fmt in schema)
        vals = list(struct.unpack(fmt_str, buf))
        out = []
        for (_, fmt) in schema:
            if 's' in fmt:
                size = int(fmt[:-1])
                raw = vals.pop(0)
                if isinstance(raw, bytes):
                    out.append(raw.rstrip(b'\x00').decode('utf-8', errors='replace'))
                else:
                    out.append(str(raw))  
            elif fmt[:-1].isdigit():
                n = int(fmt[:-1])
                out.append(tuple(vals[:n]))
                del vals[:n]
            else:
                out.append(vals.pop(0))
        return Record(schema, out)

    
    @staticmethod
    def get_size(schema) -> int:
        format_str = ''.join('i' if fmt == 'text' else fmt for _, fmt in schema)
        return struct.calcsize(format_str)

    def __str__(self) -> None:
        out_parts = []
        for (_, fmt), value in zip(self.schema, self.values):
            if 's' in fmt:                         # cadena fija
                out_parts.append(str(value))
            elif fmt[:-1].isdigit():               # '4f', '3i', etc.
                # Formatear cada componente según su tipo base
                base = fmt[-1]
                if base == 'f':
                    comp = ", ".join(f"{v:.2f}" for v in value)
                else:
                    comp = ", ".join(str(v) for v in value)
                out_parts.append(f"({comp})")
            elif fmt[-1] == 'f':                   # float suelto
                out_parts.append(f"{value:.2f}")
            else:                                  # int, etc.
                out_parts.append(str(value))
        return (" | ".join(out_parts))


def main():

    # Crear varios registros
    schema = [("id", "i"), ("nombre", "20s"), ("precio", "f"), ("cantidad", "i"), ("dbox", "4f")]

    values1 = [1, "Galletas", 3.5, 10, (2,3,4,1)]
    values2 = [2, "Chocolate", 5.2, 8, (2,1,2,3)]
    values3 = [3, "Caramelos", 1.75, 25, (1,2,3,4)]
    values4 = [4, "Cereal", 4.0, 12, (3,4,5,6)]

    # Crear los objetos Record
    registro1 = Record(schema, values1)
    registro2 = Record(schema, values2)
    registro3 = Record(schema, values3)
    registro4 = Record(schema, values4)

    # Empaquetar y desempacar (simulando lectura binaria)
    registros_bin = [
        registro1.pack(),
        registro2.pack(),
        registro3.pack(),
        registro4.pack()
    ]

    registros = [Record.unpack(bin_data, schema) for bin_data in registros_bin]

    # Mostrar todos los registros
    for r in registros:
        r.print()
