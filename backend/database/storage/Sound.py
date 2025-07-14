import struct
import os

class Sound:
    """Manejo de almacenamiento externo de rutas a archivos de sonido."""

    INT_SIZE = 4
    SENTINEL = -1  # Valor de n para indicar eliminación lógica

    def __init__(self, table_name: str, field_name: str):
        self.filename = f"{table_name}.{field_name}.dat"
        if not os.path.exists(self.filename):
            raise FileNotFoundError(f"Archivo {self.filename} no existe. Llame a build_file primero.")

    @staticmethod
    def build_file(table_name: str, field_name: str) -> None:
        """Crea el archivo <table_name>.<field_name>.dat vacío si no existe."""
        filename = f"{table_name}.{field_name}.dat"
        if not os.path.exists(filename):
            print(f"Creating sound file: {filename}")
            with open(filename, "wb") as f:
                pass

    def insert(self, text: str, histogram: list[tuple[int, int]]) -> int:
        encoded_text = text.encode("utf-8")
        n_text = len(encoded_text)

        num_tuples = len(histogram)

        with open(self.filename, "ab") as f:
            offset = f.tell()
            # Escribir longitud del texto y el texto
            f.write(struct.pack("i", n_text))
            f.write(encoded_text)

            # Escribir cantidad de tuplas
            f.write(struct.pack("i", num_tuples))

            # Escribir cada tupla (ID, COUNT)
            for centroid_id, count in histogram:
                f.write(struct.pack("ii", centroid_id, count))

        return offset

    def delete(self, offset: int) -> bool:
        try:
            with open(self.filename, "r+b") as f:
                f.seek(offset)
                f.write(struct.pack("i", self.SENTINEL))  # marcar como eliminado
            return True
        except Exception:
            return False

    def read(self, offset: int) -> tuple[str | None, list[tuple[int, int]]]:
        with open(self.filename, "rb") as f:
            f.seek(offset)

            # Leer longitud del texto
            n_bytes = f.read(self.INT_SIZE)
            if len(n_bytes) < self.INT_SIZE:
                return None, []
            (n_text,) = struct.unpack("i", n_bytes)

            if n_text == self.SENTINEL:
                return None, []

            # Leer texto
            text_content = f.read(n_text).decode("utf-8", errors="replace")

            # Leer cantidad de tuplas
            num_tuples_bytes = f.read(self.INT_SIZE)
            if len(num_tuples_bytes) < self.INT_SIZE:
                return text_content, []
            (num_tuples,) = struct.unpack("i", num_tuples_bytes)

            # Leer tuplas
            histogram = []
            for _ in range(num_tuples):
                tuple_bytes = f.read(struct.calcsize("ii"))
                if len(tuple_bytes) < struct.calcsize("ii"):
                    break
                centroid_id, count = struct.unpack("ii", tuple_bytes)
                histogram.append((centroid_id, count))

            return text_content, histogram
