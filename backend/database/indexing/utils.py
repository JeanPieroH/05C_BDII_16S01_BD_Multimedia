from .IndexRecord import IndexRecord
import os
import json
from typing import Dict, Any, Union

def load_schema(base_filename: str) -> Dict[str, Any]:
    """Carga el schema desde el archivo .schema.json
    
    Args:
        base_filename: Nombre base del archivo (sin extensión o con .dat)
    
    Returns:
        Diccionario con la estructura del schema
    
    Raises:
        FileNotFoundError: Si no existe el archivo de schema
    """
    # Asegurarnos de que tenemos el nombre base sin extensión
    schema_file = f"{base_filename}.schema.json"
    
    if not os.path.exists(schema_file):
        raise FileNotFoundError(f"Archivo de schema no encontrado: {schema_file}")
    
    with open(schema_file, "r") as f:
        return json.load(f)
    

def get_key_format_from_schema(schema: Dict[str, Any], key_field: str) -> str:
    """Obtiene el formato del campo indexado del schema
    
    Args:
        schema: Diccionario con la estructura del schema
        key_field: Nombre del campo a indexar
    
    Returns:
        Formato del campo (ej: 'i', 'f', '10s')
    
    Raises:
        ValueError: Si el campo no existe en el schema
    """
    for field in schema["fields"]:
        if field["name"] == key_field:
            return field["type"]
    available_fields = [f["name"] for f in schema["fields"]]
    raise ValueError(f"Campo '{key_field}' no encontrado en el schema. Campos disponibles: {available_fields}")

def get_default_key(fmt):
    """Devuelve un valor por defecto según el tipo de clave
    
    Args:
        key_format: Formato del campo ('i', 'f' o 'Ns')
    
    Returns:
        Valor por defecto para el tipo (0, 0.0 o "")
    
    Raises:
        ValueError: Si el formato no es soportado
    """
    if fmt == 'i':
        return 0
    elif fmt == 'f':
        return 0.0
    elif fmt.endswith('s'):
        return ''
    else:
        raise ValueError("Formato no soportado")

def get_empty_record(key_format: str) -> IndexRecord:
    """Crea un registro vacío según el tipo de clave
    
    Args:
        key_format: Formato del campo ('i', 'f' o 'Ns')
    
    Returns:
        IndexRecord configurado como "eliminado" según su tipo
    """
    empty_key = -1 if key_format == 'i' else '' if 's' in key_format else -1.0
    return IndexRecord(key_format, empty_key, 0)