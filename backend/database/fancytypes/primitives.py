"""Primitive types, attempts to be disk-safe."""

class Varchar:
    """
    String wrapper with length control.
    """

    def __init__(self, value: str, length: int = 255):
        if len(value) > length:
            raise ValueError(f"Varchar value exceeds maximum length of {length}")
        self.__value = value
        self.__length = length

    def get_value(self) -> str:
        return str(self)
    
    def set_value(self, value: str) -> None:
        if len(value) > self.__length:
            raise ValueError(f"Varchar value exceeds maximum length of {self.__length}")
        self.__value = value

    def get_length(self) -> int:
        return self.__length
    
    def set_length(self, length: int) -> None:
        raise AssertionError("Length of a Varchar cannot be changed")

    def __str__(self):
        return self.__value.ljust(self.__length)[:self.__length]

    def __repr__(self):
        return f"Varchar({self.__value}, {self.__length})"
    
    def __len__(self):
        return len(self.__value)
    
    def __eq__(self, other):
        if isinstance(other, Varchar):
            return self.__value == other.__value
        elif isinstance(other, str):
            return self.__value == other
        return False
    
    def encode(self, format='utf-8') -> bytes:
        """
        Adaps existing .encode() that str type already has.
        """
        return self.__str__().encode(format)
    
def get_FORMAT(item: object) -> str:
    """
    Returns FORMAT string.
    """
    if isinstance(item, int):
        return 'i'
    elif isinstance(item, float):
        return 'f'
    elif isinstance(item, str):
        return f"{len(item)}s"
    else:
        raise ValueError(f"Unsupported type: {type(item)}")