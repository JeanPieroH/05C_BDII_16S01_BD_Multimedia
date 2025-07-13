from enum import Enum, auto


# region Token and Scanner
class TokenType(Enum):
    CREATE = auto()
    TABLE = auto()
    INDEX = auto()
    ON = auto()
    USING = auto()
    SELECT = auto()
    FROM = auto()
    WHERE = auto()
    GROUP = auto()
    BY = auto()
    ORDER = auto()
    LIMIT = auto()
    DROP = auto()
    DELETE = auto()
    INSERT = auto()
    INTO = auto()
    VALUES = auto()
    UPDATE = auto()
    SET = auto()
    AS = auto()
    PRIMARY = auto()
    KEY = auto()
    UNIQUE = auto()
    BETWEEN = auto()
    AND = auto()
    OR = auto()
    NOT = auto()
    ASC = auto()
    DESC = auto()

    BPLUSTREE = auto()
    EXTENDIBLEHASH = auto()
    RTREE = auto()
    SEQUENTIAL = auto()

    # types
    INT = auto()
    FLOAT = auto()
    VARCHAR = auto()
    DATE = auto()
    BOOL = auto()

    EQUAL = auto()  # ==
    LESS_THAN = auto()  # <
    GREATER_THAN = auto()  # >
    LESS_EQUAL = auto()  # <=
    GREATER_EQUAL = auto()  # >=
    NOT_EQUAL = auto()  # !=
    ASSIGN = auto()  # =

    SEMICOLON = auto()  # ;
    LEFT_PARENTHESIS = auto()  # (
    RIGHT_PARENTHESIS = auto()  # )
    COMMA = auto()  # ,
    DOT = auto()  # .

    ASTERISK = auto()
    USER_IDENTIFIER = auto()

    INT_CONSTANT = auto()  # VARCHAR(INT_CONSTANT), LIMIT INT_CONSTANT
    FLOAT_CONSTANT = auto()
    STRING_CONSTANT = auto()

    TRUE = auto()
    FALSE = auto()
    DATE_CONSTANT = auto()

    END = auto()
    ERROR = auto()

    POINT2D = auto()  # for RTREE
    POINT3D = auto()  # for RTREE


class Token:
    TYPE_TO_TEXT = {
        TokenType.CREATE: "CREATE",
        TokenType.TABLE: "TABLE",
        TokenType.INDEX: "INDEX",
        TokenType.ON: "ON",
        TokenType.USING: "USING",
        TokenType.SELECT: "SELECT",
        TokenType.FROM: "FROM",
        TokenType.WHERE: "WHERE",
        TokenType.GROUP: "GROUP",
        TokenType.BY: "BY",
        TokenType.ORDER: "ORDER",
        TokenType.LIMIT: "LIMIT",
        TokenType.DROP: "DROP",
        TokenType.DELETE: "DELETE",
        TokenType.INSERT: "INSERT",
        TokenType.INTO: "INTO",
        TokenType.VALUES: "VALUES",
        TokenType.UPDATE: "UPDATE",
        TokenType.SET: "SET",
        TokenType.AS: "AS",
        TokenType.PRIMARY: "PRIMARY",
        TokenType.KEY: "KEY",
        TokenType.UNIQUE: "UNIQUE",
        TokenType.BETWEEN: "BETWEEN",
        TokenType.AND: "AND",
        TokenType.OR: "OR",
        TokenType.NOT: "NOT",
        TokenType.ASC: "ASC",
        TokenType.DESC: "DESC",
        TokenType.BPLUSTREE: "BPLUSTREE",
        TokenType.EXTENDIBLEHASH: "HASHFILE",
        TokenType.RTREE: "RTREE",
        TokenType.SEQUENTIAL: "SEQUENTIAL",
        TokenType.INT: "INT",
        TokenType.FLOAT: "FLOAT",
        TokenType.VARCHAR: "VARCHAR",
        TokenType.DATE: "DATE",
        TokenType.BOOL: "BOOL",
        TokenType.TRUE: "TRUE",
        TokenType.FALSE: "FALSE",
        TokenType.EQUAL: "EQUAL",
        TokenType.NOT_EQUAL: "NOT_EQUAL",
        TokenType.ASSIGN: "ASSIGN",
        TokenType.LESS_THAN: "LESS_THAN",
        TokenType.GREATER_THAN: "GREATER_THAN",
        TokenType.LESS_EQUAL: "LESS_EQUAL",
        TokenType.GREATER_EQUAL: "GREATER_EQUAL",
        TokenType.SEMICOLON: "SEMICOLON",
        TokenType.LEFT_PARENTHESIS: "LEFT_PARENTHESIS",
        TokenType.RIGHT_PARENTHESIS: "RIGHT_PARENTHESIS",
        TokenType.COMMA: "COMMA",
        TokenType.DOT: "DOT",
        TokenType.ASTERISK: "ASTERISK",
        TokenType.USER_IDENTIFIER: "USER_IDENTIFIER",
        TokenType.INT_CONSTANT: "INT_CONSTANT",
        TokenType.FLOAT_CONSTANT: "FLOAT_CONSTANT",
        TokenType.STRING_CONSTANT: "STRING_CONSTANT",
        TokenType.DATE_CONSTANT: "DATE_CONSTANT",
        TokenType.END: "END",
        TokenType.ERROR: "ERROR",
        # aaaa
        TokenType.POINT2D: "POINT2D",
        TokenType.POINT3D: "POINT3D",
    }

    TEXT_TO_TYPE = {text: key for key, text in TYPE_TO_TEXT.items()}

    def __init__(self, token_type: TokenType, text: str = ""):
        self.token_type = token_type
        self.text = text

    def __str__(self):
        if self.token_type in Token.TYPE_TO_TEXT:
            return f"Token({Token.TYPE_TO_TEXT[self.token_type]}, {self.text})"
        else:
            return f"Token({self.token_type.name}, {self.text})"


class Scanner:
    def __init__(self, source: str):
        self.source = source
        self.position = 0
        self.current_char = self.source[self.position] if self.source else None

    def is_postgres_date(self, text: str) -> bool:
        import re

        # Pattern: exactly 4 digits, dash, 2 digits, dash, 2 digits
        date_pattern = r"^\d{4}-\d{2}-\d{2}$"

        if not re.match(date_pattern, text):
            return False

        try:
            year, month, day = text.split("-")
            year, month, day = int(year), int(month), int(day)

            if month < 1 or month > 12:
                return False
            if day < 1 or day > 31:
                return False

            return True
        except Exception:
            return False

    def advance(self):
        self.position += 1
        if self.position < len(self.source):
            self.current_char = self.source[self.position]
        else:
            self.current_char = None

    def skip_whitespace(self):
        while self.current_char is not None and self.current_char.isspace():
            self.advance()

    def next_token(self) -> Token:
        while self.current_char is not None:
            if self.current_char.isspace():
                self.skip_whitespace()
                continue

            # ids, keywords
            if self.current_char.isalpha():
                start_pos = self.position
                while self.current_char is not None and (
                    self.current_char.isalnum() or self.current_char == "_"
                ):
                    self.advance()
                text = self.source[start_pos : self.position]
                token_type = Token.TEXT_TO_TYPE.get(
                    text.upper(), TokenType.USER_IDENTIFIER
                )
                return Token(token_type, text)

            # Handle numeric constants
            if self.current_char.isdigit():
                start_pos = self.position
                while self.current_char is not None and self.current_char.isdigit():
                    self.advance()
                if self.current_char == ".":
                    self.advance()
                    while self.current_char is not None and self.current_char.isdigit():
                        self.advance()
                    text = self.source[start_pos : self.position]
                    return Token(TokenType.FLOAT_CONSTANT, text)
                else:
                    text = self.source[start_pos : self.position]
                    return Token(TokenType.INT_CONSTANT, text)

            # Handle operators and symbols
            if self.current_char == "=":
                if (
                    self.position + 1 < len(self.source)
                    and self.source[self.position + 1] == "="
                ):
                    self.advance()
                    self.advance()
                    return Token(TokenType.EQUAL, "==")
                else:
                    self.advance()
                    return Token(TokenType.ASSIGN, "=")
            if self.current_char == "<":
                if (
                    self.position + 1 < len(self.source)
                    and self.source[self.position + 1] == "="
                ):
                    self.advance()
                    self.advance()
                    return Token(TokenType.LESS_EQUAL, "<=")
                else:
                    self.advance()
                    return Token(TokenType.LESS_THAN, "<")
            if self.current_char == ">":
                if (
                    self.position + 1 < len(self.source)
                    and self.source[self.position + 1] == "="
                ):
                    self.advance()
                    self.advance()
                    return Token(TokenType.GREATER_EQUAL, ">=")
                else:
                    self.advance()
                    return Token(TokenType.GREATER_THAN, ">")
            if self.current_char == "!":
                if (
                    self.position + 1 < len(self.source)
                    and self.source[self.position + 1] == "="
                ):
                    self.advance()
                    self.advance()
                    return Token(TokenType.NOT_EQUAL, "!=")
                else:
                    self.advance()
                    return Token(TokenType.ERROR, "Invalid character !")
            if self.current_char == ";":
                self.advance()
                return Token(TokenType.SEMICOLON, ";")
            if self.current_char == "(":
                self.advance()
                return Token(TokenType.LEFT_PARENTHESIS, "(")
            if self.current_char == ")":
                self.advance()
                return Token(TokenType.RIGHT_PARENTHESIS, ")")
            if self.current_char == ",":
                self.advance()
                return Token(TokenType.COMMA, ",")
            if self.current_char == ".":
                self.advance()
                return Token(TokenType.DOT, ".")
            if self.current_char == "*":
                self.advance()
                return Token(TokenType.ASTERISK, "*")
            if self.current_char == "'":  # for single quote strings
                self.advance()
                start_pos = self.position
                while self.current_char is not None and self.current_char != "'":
                    self.advance()
                if self.current_char == "'":
                    text = self.source[start_pos : self.position]
                    self.advance()
                    if self.is_postgres_date(text):
                        return Token(TokenType.DATE_CONSTANT, text)
                    else:
                        return Token(TokenType.STRING_CONSTANT, text)
                else:
                    return Token(TokenType.ERROR, "Unterminated string constant")
            if self.current_char == '"':  # double quote strings just in case
                self.advance()
                start_pos = self.position
                while self.current_char is not None and self.current_char != '"':
                    self.advance()
                if self.current_char == '"':
                    text = self.source[start_pos : self.position]
                    self.advance()
                    if self.is_postgres_date(text):
                        return Token(TokenType.DATE_CONSTANT, text)
                    else:
                        return Token(TokenType.STRING_CONSTANT, text)
                else:
                    return Token(TokenType.ERROR, "Unterminated string constant")
            if self.current_char == "-":
                self.advance()
                if self.current_char == "-":
                    # single line comment
                    while self.current_char is not None and self.current_char != "\n":
                        self.advance()
                    continue
            if self.current_char == "/":
                self.advance()
                if self.current_char == "*":
                    # multiline
                    self.advance()
                    while self.current_char is not None:
                        if self.current_char == "*" and (
                            self.position + 1 < len(self.source)
                            and self.source[self.position + 1] == "/"
                        ):
                            self.advance()
                            self.advance()
                            break
                        self.advance()
                    continue
            return Token(
                TokenType.ERROR, f"Unrecognized character: {self.current_char}"
            )
        return Token(TokenType.END)

    def test(self):
        scanner = Scanner(self.source)
        token = scanner.next_token()
        while token.token_type != TokenType.END:
            if token.token_type == TokenType.ERROR:
                print(f"Error: {token.text}")
                break
            else:
                print(token)
                token = scanner.next_token()


# endregion
