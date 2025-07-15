import struct
import os

class HistogramFile:
    """Manejo de almacenamiento externo de histogramas."""

    INT_SIZE = 4
    SENTINEL = -1  # Valor para indicar eliminación lógica

    def __init__(self, table_name: str, field_name: str):
        self.filename = os.path.join("backend/database/tables", f"{table_name}.{field_name}.histogram.dat")
        if not os.path.exists(self.filename):
            raise FileNotFoundError(f"Archivo {self.filename} no existe. Llame a build_file primero.")

    @staticmethod
    def build_file(table_name: str, field_name: str) -> None:
        """Crea el archivo <table_name>.<field_name>.histogram.dat vacío si no existe."""
        filename = os.path.join("backend/database/tables", f"{table_name}.{field_name}.histogram.dat")
        if not os.path.exists(filename):
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, "wb") as f:
                pass

    def insert(self, histogram: list[tuple[int, int]]) -> int:
        num_tuples = len(histogram)
        with open(self.filename, "ab") as f:
            offset = f.tell()
            f.write(struct.pack("i", num_tuples))
            for centroid_id, count in histogram:
                f.write(struct.pack("ii", centroid_id, count))
        return offset

    def read(self, offset: int) -> list[tuple[int, int]]:
        with open(self.filename, "rb") as f:
            f.seek(offset)
            num_tuples_bytes = f.read(self.INT_SIZE)
            if len(num_tuples_bytes) < self.INT_SIZE:
                return []
            (num_tuples,) = struct.unpack("i", num_tuples_bytes)

            histogram = []
            for _ in range(num_tuples):
                tuple_bytes = f.read(struct.calcsize("ii"))
                if len(tuple_bytes) < struct.calcsize("ii"):
                    break
                centroid_id, count = struct.unpack("ii", tuple_bytes)
                histogram.append((centroid_id, count))
            return histogram
