from enum import Enum, auto


class ColumnType(Enum):
    INT = auto()
    FLOAT = auto()
    VARCHAR = auto()
    DATE = auto()
    BOOL = auto()
    POINT2D = auto()
    POINT3D = auto()

    def __str__(self):
        return self.name


class OperationType(Enum):
    EQUAL = auto()  # =
    NOT_EQUAL = auto()  # !=
    GREATER_THAN = auto()  # >
    LESS_THAN = auto()  # <
    GREATER__EQUAL = auto()  # >=
    LESS__EQUAL = auto()  # <=
    IN = auto()  # IN
    BETWEEN = auto()  # BETWEEN

    def __str__(self):
        match self:
            case OperationType.EQUAL:
                return "="
            case OperationType.NOT_EQUAL:
                return "!="
            case OperationType.GREATER_THAN:
                return ">"
            case OperationType.LESS_THAN:
                return "<"
            case OperationType.GREATER__EQUAL:
                return ">="
            case OperationType.LESS__EQUAL:
                return "<="
            case OperationType.IN:
                return "IN"
            case OperationType.BETWEEN:
                return "BETWEEN"


class IndexType(Enum):
    BPLUSTREE = auto()  # int, float, string
    EXTENDIBLEHASH = auto()  # int or string
    RTREE = auto()  # dunno
    SEQUENTIAL = auto()  # int, float, string

    def __str__(self):
        return self.name


class QueryResult:
    def __init__(self, success: bool, message: str = "", data=None):
        self.success = success
        self.message = message
        self.data = data

    def __repr__(self):
        return f"QueryResult(success={self.success}, message='{self.message}', data={self.data})"
