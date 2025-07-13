import struct

SchemaType = list[tuple[str, str]] # should not be empty

def get_size(schema: SchemaType) -> int:
    format_str = ''.join(fmt for _, fmt in schema)
    return struct.calcsize(format_str)