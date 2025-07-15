import struct
import os

class Sound:
    """Manejo de almacenamiento externo de rutas a archivos de sonido."""

    INT_SIZE = 4
    SENTINEL = -1  # Valor de n para indicar eliminación lógica

    def __init__(self, table_name: str, field_name: str):
        self.filename = os.path.join("backend/database/tables", f"{table_name}.{field_name}.dat")
        if not os.path.exists(self.filename):
            raise FileNotFoundError(f"Archivo {self.filename} no existe. Llame a build_file primero.")

    @staticmethod
    def build_file(table_name: str, field_name: str) -> None:
        """Crea el archivo <table_name>.<field_name>.dat vacío si no existe."""
        filename = os.path.join("backend/database/tables", f"{table_name}.{field_name}.dat")
        if not os.path.exists(filename):
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, "wb") as f:
                pass

    def insert(self, text: str) -> int:
        encoded = text.encode("utf-8")
        n = len(encoded)
        with open(self.filename, "ab") as f:
            offset = f.tell()
            f.write(struct.pack("i", n))  # escribe el prefijo n
            f.write(encoded)
        return offset

    def delete(self, offset: int) -> bool:
        try:
            with open(self.filename, "r+b") as f:
                f.seek(offset)
                f.write(struct.pack("i", self.SENTINEL))  # marcar como eliminado
            return True
        except Exception:
            return False

    def read(self, offset: int) -> str | None:
        try:
            with open(self.filename, "rb") as f:
                if offset < 0 or offset >= os.fstat(f.fileno()).st_size:
                    return None
                f.seek(offset)
                n_bytes = f.read(self.INT_SIZE)
                if len(n_bytes) < self.INT_SIZE:
                    return None
                (n,) = struct.unpack("i", n_bytes)
                if n == self.SENTINEL or n <= 0:
                    return None
                content = f.read(n)
                return content.decode("utf-8", errors="ignore")
        except (IOError, struct.error):
            return None
