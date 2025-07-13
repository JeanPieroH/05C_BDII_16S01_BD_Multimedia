from visitor import PrintVisitor, RunVisitor
from scanner import Scanner, Token, TokenType
from column_types import ColumnType, QueryResult, OperationType
from statement import (
    Statement,
    Program,
    CreateTableStatement,
    DropTableStatement,
    CreateIndexStatement,
    DropIndexStatement,
    CreateColumnDefinition,
    InsertStatement,
    IndexType,
    ConstantExpression,
    IntExpression,
    FloatExpression,
    BoolExpression,
    StringExpression,
    SelectStatement,
    WhereStatement,
    ValueExpression,
    ColumnExpression,
    Point2DExpression,
    Point3DExpression,
    # sadsads
    OrCondition,
    AndCondition,
    NotCondition,
    PrimaryCondition,
    ConstantCondition,
    SimpleComparison,
    BetweenComparison,
)
import time
import random
from faker import Faker


def operation_token_to_type(token_type: TokenType) -> OperationType:
    if (
        token_type == TokenType.EQUAL or token_type == TokenType.ASSIGN
    ):  # used interchangeably aadssadsadsa
        return OperationType.EQUAL
    elif token_type == TokenType.NOT_EQUAL:
        return OperationType.NOT_EQUAL
    elif token_type == TokenType.GREATER_THAN:
        return OperationType.GREATER_THAN
    elif token_type == TokenType.LESS_THAN:
        return OperationType.LESS_THAN
    elif token_type == TokenType.GREATER_EQUAL:
        return OperationType.GREATER__EQUAL
    elif token_type == TokenType.LESS_EQUAL:
        return OperationType.LESS__EQUAL
    else:
        raise ValueError(f"Invalid operation token: {token_type}")


class Parser:
    def __init__(self, scanner: Scanner, debug: bool = False):
        self.debug = debug
        self.scanner = scanner
        self.prev: Token = None
        self.curr: Token = self.scanner.next_token()

    def print_debug(self, message: str):
        if self.debug:
            print(f"DEBUG: {message} | Current Token: {self.curr}")

    def check(self, token_type: TokenType) -> bool:
        if self.is_at_end():
            return False
        return self.curr.token_type == token_type

    def match(self, token_type: TokenType) -> bool:
        if self.check(token_type):
            self.advance()
            return True
        return False

    def is_at_end(self) -> bool:
        return self.curr.token_type == TokenType.END

    def advance(self) -> bool:
        if not self.is_at_end():
            temp: Token = self.curr
            self.curr = self.scanner.next_token()
            self.prev = temp
            if self.check(TokenType.ERROR):
                raise SyntaxError(f"Syntax error at token: {self.curr.text}")
            return True
        return False

    def parse_column_definition(self) -> CreateColumnDefinition:
        self.print_debug("Parsing column definition")

        column_name, column_type, varchar_length, is_pk = None, None, None, False

        if not self.match(TokenType.USER_IDENTIFIER):
            raise SyntaxError(f"Expected column name, found {self.curr.text}")

        column_name = self.prev.text

        if self.match(TokenType.INT):
            column_type = ColumnType.INT
        elif self.match(TokenType.FLOAT):
            column_type = ColumnType.FLOAT
        elif self.match(TokenType.BOOL):
            column_type = ColumnType.BOOL
        elif self.match(TokenType.DATE):
            column_type = ColumnType.DATE
        elif self.match(TokenType.POINT2D):
            column_type = ColumnType.POINT2D  # TODO: integrate rtree
        elif self.match(TokenType.POINT3D):
            column_type = ColumnType.POINT3D  # TODO: integrate rtree
        elif self.match(TokenType.VARCHAR):
            if not self.match(TokenType.LEFT_PARENTHESIS):
                raise SyntaxError(f"Expected '(' after VARCHAR, found {self.curr.text}")
            if not self.match(TokenType.INT_CONSTANT):
                raise SyntaxError(
                    f"Expected integer constant for VARCHAR length, found {self.curr.text}"
                )
            varchar_length = int(self.prev.text)
            if varchar_length <= 0:
                raise SyntaxError(
                    f"Invalid VARCHAR length {varchar_length}, must be greater than 0"
                )
            if not self.match(TokenType.RIGHT_PARENTHESIS):
                raise SyntaxError(
                    f"Expected ')' after VARCHAR length, found {self.curr.text}"
                )
            column_type = ColumnType.VARCHAR

        if column_type not in (
            ColumnType.INT,
            ColumnType.FLOAT,
            ColumnType.VARCHAR,
            ColumnType.DATE,
            ColumnType.BOOL,
            ColumnType.POINT2D,
            ColumnType.POINT3D,
        ):
            raise SyntaxError(
                f"Invalid column type {self.prev.text} for column {column_name}"
            )

        if self.match(TokenType.PRIMARY):
            if not self.match(TokenType.KEY):
                raise SyntaxError(f"Expected KEY after PRIMARY, found {self.curr.text}")
            is_pk = True

        # TODO: allow for index usage in CREATE TABLE

        return CreateColumnDefinition(column_name, column_type, varchar_length, is_pk)

    def parse_column_definition_list(self) -> list[CreateColumnDefinition]:
        self.print_debug("Parsing column definition list")
        columns: list[CreateColumnDefinition] = []

        while not self.check(TokenType.RIGHT_PARENTHESIS):
            columns.append(self.parse_column_definition())
            if self.check(TokenType.COMMA):
                self.advance()

        if len(columns) == 0:
            raise SyntaxError("At least one column definition is required")

        return columns

    # TODO: primary key
    def parse_create_table_statement(self) -> CreateTableStatement:
        self.print_debug("Parsing CREATE TABLE statement")
        table_name = None
        if not self.match(TokenType.USER_IDENTIFIER):
            raise SyntaxError(
                f"Expected table name after CREATE TABLE, found {self.curr.text}"
            )
        table_name = self.prev.text

        if not self.match(TokenType.LEFT_PARENTHESIS):
            raise SyntaxError(f"Expected '(' after table name, found {self.curr.text}")
        columns: list[CreateColumnDefinition] = self.parse_column_definition_list()
        if not self.match(TokenType.RIGHT_PARENTHESIS):
            raise SyntaxError(
                f"Expected ')' after column definitions, found {self.curr.text}"
            )
        return CreateTableStatement(table_name, columns)

    def parse_drop_table_statement(self) -> DropTableStatement:
        self.print_debug("Parsing DROP TABLE statement")
        if not self.match(TokenType.USER_IDENTIFIER):
            raise SyntaxError(
                f"Expected table name after DROP TABLE, found {self.curr.text}"
            )
        table_name = self.prev.text
        return DropTableStatement(table_name)

    def parse_create_index_statement(self) -> CreateIndexStatement:
        self.print_debug("Parsing CREATE INDEX statement")

        # current index implementation doesnt support index name, so we omit it
        # if not self.match(TokenType.USER_IDENTIFIER):
        #     raise SyntaxError(f"Expected index name after CREATE INDEX, found {self.curr.text}")
        # index_name = self.prev.text
        # TODO: change index creation functions to accept and use index name

        if not self.match(TokenType.ON):
            raise SyntaxError(f"Expected ON after index name, found {self.curr.text}")

        if not self.match(TokenType.USER_IDENTIFIER):
            raise SyntaxError(f"Expected table name after ON, found {self.curr.text}")
        table_name = self.prev.text

        if not self.match(TokenType.LEFT_PARENTHESIS):
            raise SyntaxError(f"Expected '(' after table name, found {self.curr.text}")

        if not self.match(TokenType.USER_IDENTIFIER):
            raise SyntaxError(f"Expected column name after '(', found {self.curr.text}")
        column_name = self.prev.text

        if not self.match(TokenType.RIGHT_PARENTHESIS):
            raise SyntaxError(f"Expected ')' after column name, found {self.curr.text}")

        if not self.match(TokenType.USING):
            raise SyntaxError(
                f"Expected USING after column name, found {self.curr.text}"
            )

        if self.match(TokenType.BPLUSTREE):
            index_type = IndexType.BPLUSTREE
        elif self.match(TokenType.EXTENDIBLEHASH):
            index_type = IndexType.EXTENDIBLEHASH
        elif self.match(TokenType.RTREE):
            index_type = IndexType.RTREE
        elif self.match(TokenType.SEQUENTIAL):
            index_type = IndexType.SEQUENTIAL
        else:
            raise SyntaxError(
                f"Expected index type (BPLUSTREE, EXTENDIBLEHASH, RTREE, SEQUENTIAL), found {self.curr.text}"
            )
        return CreateIndexStatement("index_name", table_name, column_name, index_type)

    def parse_drop_index_statement(self) -> DropIndexStatement:
        self.print_debug("Parsing DROP INDEX statement")

        if self.match(TokenType.BPLUSTREE):
            index_type = IndexType.BPLUSTREE
        elif self.match(TokenType.EXTENDIBLEHASH):
            index_type = IndexType.EXTENDIBLEHASH
        elif self.match(TokenType.RTREE):
            index_type = IndexType.RTREE
        elif self.match(TokenType.SEQUENTIAL):
            index_type = IndexType.SEQUENTIAL
        else:
            raise SyntaxError(
                f"Expected index type (BPLUSTREE, EXTENDIBLEHASH, RTREE, SEQUENTIAL), found {self.curr.text}"
            )

        if not self.match(TokenType.ON):
            raise SyntaxError(f"Expected ON after index type, found {self.curr.text}")

        if not self.match(TokenType.USER_IDENTIFIER):
            raise SyntaxError(f"Expected table name after ON, found {self.curr.text}")
        table_name = self.prev.text

        if not self.match(TokenType.LEFT_PARENTHESIS):
            raise SyntaxError(f"Expected '(' after table name, found {self.curr.text}")

        if not self.match(TokenType.USER_IDENTIFIER):
            raise SyntaxError(f"Expected column name after '(', found {self.curr.text}")
        column_name = self.prev.text

        if not self.match(TokenType.RIGHT_PARENTHESIS):
            raise SyntaxError(f"Expected ')' after column name, found {self.curr.text}")

        return DropIndexStatement(index_type, table_name, column_name)

    def parse_insert_statement_columns(self) -> list[str]:
        columns: list[str] = []
        while not self.check(TokenType.RIGHT_PARENTHESIS):
            if not self.match(TokenType.USER_IDENTIFIER):
                raise SyntaxError(f"Expected column name, found {self.curr.text}")
            columns.append(self.prev.text)

            if self.check(TokenType.COMMA):
                self.advance()
            elif not self.check(TokenType.RIGHT_PARENTHESIS):
                raise SyntaxError(
                    f"Expected ',' or ')' after column name, found {self.curr.text}"
                )
        return columns

    def parse_point_2d_expression(self) -> Point2DExpression:
        if not self.match(TokenType.LEFT_PARENTHESIS):
            raise SyntaxError(f"Expected '(' after POINT3D, found {self.curr.text}")
        if not (
            self.match(TokenType.FLOAT_CONSTANT) or self.match(TokenType.INT_CONSTANT)
        ):
            raise SyntaxError(
                f"Expected x coordinate (FLOAT or INT) after '(', found {self.curr.text}"
            )
        point_x = float(self.prev.text)
        if not self.match(TokenType.COMMA):
            raise SyntaxError(
                f"Expected ',' after x coordinate, found {self.curr.text}"
            )
        if not (
            self.match(TokenType.FLOAT_CONSTANT) or self.match(TokenType.INT_CONSTANT)
        ):
            raise SyntaxError(
                f"Expected y coordinate (FLOAT or INT) after ',', found {self.curr.text}"
            )
        point_y = float(self.prev.text)
        if not self.match(TokenType.RIGHT_PARENTHESIS):
            raise SyntaxError(
                f"Expected ')' after y coordinate, found {self.curr.text}"
            )
        return Point2DExpression(point_x, point_y)

    def parse_point_3d_expression(self) -> Point3DExpression:
        if not self.match(TokenType.LEFT_PARENTHESIS):
            raise SyntaxError(f"Expected '(' after POINT3D, found {self.curr.text}")
        if not (
            self.match(TokenType.FLOAT_CONSTANT) or self.match(TokenType.INT_CONSTANT)
        ):
            raise SyntaxError(
                f"Expected x coordinate (FLOAT or INT) after '(', found {self.curr.text}"
            )
        point_x = float(self.prev.text)
        if not self.match(TokenType.COMMA):
            raise SyntaxError(
                f"Expected ',' after x coordinate, found {self.curr.text}"
            )
        if not (
            self.match(TokenType.FLOAT_CONSTANT) or self.match(TokenType.INT_CONSTANT)
        ):
            raise SyntaxError(
                f"Expected y coordinate (FLOAT or INT) after ',', found {self.curr.text}"
            )
        point_y = float(self.prev.text)
        if not self.match(TokenType.COMMA):
            raise SyntaxError(
                f"Expected ',' after y coordinate, found {self.curr.text}"
            )
        if not (
            self.match(TokenType.FLOAT_CONSTANT) or self.match(TokenType.INT_CONSTANT)
        ):
            raise SyntaxError(
                f"Expected z coordinate (FLOAT or INT) after ',', found {self.curr.text}"
            )
        point_z = float(self.prev.text)
        if not self.match(TokenType.RIGHT_PARENTHESIS):
            raise SyntaxError(
                f"Expected ')' after z coordinate, found {self.curr.text}"
            )
        return Point3DExpression(point_x, point_y, point_z)

    def parse_insert_statement_values(self) -> list[ConstantExpression]:
        constants: list[ConstantExpression] = []
        while not self.check(TokenType.RIGHT_PARENTHESIS):
            if self.match(TokenType.INT_CONSTANT):
                constants.append(IntExpression(int(self.prev.text)))
            elif self.match(TokenType.FLOAT_CONSTANT):
                constants.append(FloatExpression(float(self.prev.text)))
            elif self.match(TokenType.TRUE) or self.match(TokenType.FALSE):
                constants.append(BoolExpression(self.prev.text.lower() == "true"))
            elif self.match(TokenType.STRING_CONSTANT):
                constants.append(StringExpression(self.prev.text.strip('"').strip("'")))
            elif self.match(TokenType.POINT2D):
                constants.append(self.parse_point_2d_expression())
            elif self.match(TokenType.POINT3D):
                constants.append(self.parse_point_3d_expression())
            else:
                raise SyntaxError(
                    f"Expected constant value (INT, FLOAT, BOOL, STRING, POINT2D(x,y), POINT3D(x,y,z)), found {self.curr.text}"
                )
            if self.check(TokenType.COMMA):
                self.advance()
            elif not self.check(TokenType.RIGHT_PARENTHESIS):
                raise SyntaxError(
                    f"Expected ',' or ')' after constant value, found {self.curr.text}"
                )
        return constants

    def parse_insert_statement(self) -> InsertStatement:
        if not self.match(TokenType.INTO):
            raise SyntaxError(f"Expected INTO after INSERT, found {self.curr.text}")

        if not self.match(TokenType.USER_IDENTIFIER):
            raise SyntaxError(f"Expected table name after INTO, found {self.curr.text}")
        table_name = self.prev.text

        if not self.match(TokenType.LEFT_PARENTHESIS):
            raise SyntaxError(f"Expected '(' after table name, found {self.curr.text}")

        columns: list[str] = self.parse_insert_statement_columns()

        if not self.match(TokenType.RIGHT_PARENTHESIS):
            raise SyntaxError(
                f"Expected ')' after column names, found {self.curr.text}"
            )

        if not self.match(TokenType.VALUES):
            raise SyntaxError(
                f"Expected VALUES after column names, found {self.curr.text}"
            )

        if not self.match(TokenType.LEFT_PARENTHESIS):
            raise SyntaxError(f"Expected '(' after VALUES, found {self.curr.text}")

        constants: list[ConstantExpression] = self.parse_insert_statement_values()

        if not self.match(TokenType.RIGHT_PARENTHESIS):
            raise SyntaxError(
                f"Expected ')' after constant values, found {self.curr.text}"
            )

        return InsertStatement(table_name, columns, constants)

    def parse_value_expression(self) -> ValueExpression:
        # first, constants
        if self.match(TokenType.INT_CONSTANT):
            return IntExpression(int(self.prev.text))
        elif self.match(TokenType.FLOAT_CONSTANT):
            return FloatExpression(float(self.prev.text))
        elif self.match(TokenType.TRUE) or self.match(TokenType.FALSE):
            return BoolExpression(self.prev.text.lower() == "true")
        elif self.match(TokenType.STRING_CONSTANT):
            return StringExpression(self.prev.text.strip('"').strip("'"))
        elif self.match(TokenType.POINT2D):
            return self.parse_point_2d_expression()
        elif self.match(TokenType.POINT3D):
            return self.parse_point_3d_expression()
        # then, column references
        elif self.match(TokenType.USER_IDENTIFIER):
            column_name = self.prev.text
            table_name = None
            if self.match(TokenType.DOT):
                table_name = column_name
                column_name = self.prev.text
            return ColumnExpression(column_name, table_name)
        else:
            # TODO: handle function calls or nested subqueries (im not doing nested subqueries ty very much)
            raise SyntaxError(
                f"Expected constant value (INT, FLOAT, BOOL, STRING) or column reference, found {self.curr.text}"
            )

    # region Condition Parsing and Where
    def parse_primary_condition(self) -> PrimaryCondition:
        condition = None
        # nested condition
        if self.match(TokenType.LEFT_PARENTHESIS):
            condition = self.parse_or_condition()
            if not self.match(TokenType.RIGHT_PARENTHESIS):
                raise SyntaxError(
                    f"Expected ')' after condition, found {self.curr.text}"
                )
            return PrimaryCondition(condition)

        # constant condition
        if self.match(TokenType.TRUE) or self.match(TokenType.FALSE):
            bool_value = self.prev.text.lower() == "true"
            return PrimaryCondition(ConstantCondition(BoolExpression(bool_value)))

        # else we got a simplecomp or between

        value_expr: ValueExpression = self.parse_value_expression()
        if (
            self.match(TokenType.ASSIGN)
            or self.match(TokenType.EQUAL)
            or self.match(TokenType.NOT_EQUAL)
            or self.match(TokenType.LESS_THAN)
            or self.match(TokenType.LESS_EQUAL)
            or self.match(TokenType.GREATER_THAN)
            or self.match(TokenType.GREATER_EQUAL)
        ):
            op: OperationType = operation_token_to_type(self.prev.token_type)
            right_expr = self.parse_value_expression()
            return PrimaryCondition(SimpleComparison(value_expr, op, right_expr))

        if self.match(TokenType.BETWEEN):
            lower = self.parse_value_expression()
            if not self.match(TokenType.AND):
                raise SyntaxError("Expected AND after BETWEEN lower bound")
            upper = self.parse_value_expression()
            return PrimaryCondition(BetweenComparison(value_expr, lower, upper))
        # run visitor handles invalid stuff, like string < 5 or 12.5 = "hello"
        raise SyntaxError(
            f"Expected condition (constant, simple comparison, or BETWEEN), found {self.curr.text}"
        )

    def parse_not_condition(self) -> NotCondition:
        is_not = False
        if self.match(TokenType.NOT):
            is_not = True
        primary_condition = self.parse_primary_condition()
        return NotCondition(is_not, primary_condition)

    def parse_and_condition(self) -> AndCondition:
        not_condition = self.parse_not_condition()
        and_condition = None
        if self.match(TokenType.AND):
            and_condition = self.parse_and_condition()
        return AndCondition(not_condition, and_condition)

    def parse_or_condition(self) -> OrCondition:
        and_condition = self.parse_and_condition()
        or_condition = None
        if self.match(TokenType.OR):
            or_condition = self.parse_or_condition()

        return OrCondition(and_condition, or_condition)

    def parse_where_statement(self) -> WhereStatement:
        or_condition: OrCondition = self.parse_or_condition()
        return WhereStatement(or_condition)

    # endregion
    def parse_select_list(self) -> list[str]:
        select_columns: list[str] = []

        while not self.check(TokenType.FROM):
            table_name, column_name = None, None

            if not self.match(TokenType.USER_IDENTIFIER):
                raise SyntaxError(f"Expected column name, found {self.curr.text}")
            column_name = self.prev.text

            if self.match(TokenType.DOT):
                if not self.match(TokenType.USER_IDENTIFIER):
                    raise SyntaxError(
                        f"Expected table name after '.', found {self.curr.text}"
                    )
                table_name = column_name
                column_name = self.prev.text
            select_columns.append(
                f"{f'{table_name}.' if table_name is not None else ''}{column_name}"
            )

            # TODO: handle column aliases

            if self.check(TokenType.COMMA):
                self.advance()
            elif not self.check(TokenType.FROM):
                raise SyntaxError(
                    f"Expected ',' or FROM after column name, found {self.curr.text}"
                )

        if len(select_columns) == 0:
            raise SyntaxError("At least one column must be selected")
        return select_columns

    def parse_select_statement(self) -> Statement:
        select_columns: list[str] = []
        from_table: str = None
        select_all = False
        where_statement: WhereStatement = None
        order_by_column: str = None
        ascending: bool = True
        limit: int = None

        # * or list of columns
        if self.match(TokenType.ASTERISK):
            select_all = True
        elif self.check(TokenType.USER_IDENTIFIER):
            select_columns = self.parse_select_list()

        if not self.match(TokenType.FROM):
            raise SyntaxError(
                f"Expected FROM after selected columns, found {self.curr.text}"
            )

        if not self.match(TokenType.USER_IDENTIFIER):
            raise SyntaxError(f"Expected table name after FROM, found {self.curr.text}")
        from_table = self.prev.text

        # TODO: handle table alias

        if self.match(TokenType.WHERE):
            where_statement = self.parse_where_statement()

        if self.match(TokenType.ORDER):
            if not self.match(TokenType.BY):
                raise SyntaxError(f"Expected BY after ORDER, found {self.curr.text}")
            if not self.match(TokenType.USER_IDENTIFIER):
                raise SyntaxError(
                    f"Expected column name after ORDER BY, found {self.curr.text}"
                )
            order_by_column = self.prev.text
            ascending = True
            if self.match(TokenType.DESC):
                ascending = False

        if self.match(TokenType.LIMIT):
            if not self.match(TokenType.INT_CONSTANT):
                raise SyntaxError(
                    f"Expected integer constant after LIMIT, found {self.curr.text}"
                )
            limit = int(self.prev.text)

        return SelectStatement(
            select_columns,
            from_table,
            select_all,
            where_statement,
            order_by_column,
            ascending,
            limit,
        )

    def parse_update_statement(self) -> Statement:
        raise NotImplementedError("UPDATE statement parsing is not implemented yet")

    def parse_delete_statement(self) -> Statement:
        raise NotImplementedError("DELETE statement parsing is not implemented yet")

    def parse_statement(self) -> Statement:
        if self.match(TokenType.CREATE):
            if self.match(TokenType.TABLE):
                return self.parse_create_table_statement()
            elif self.match(TokenType.INDEX):
                return self.parse_create_index_statement()
            else:
                raise SyntaxError(
                    f"Expected TABLE or INDEX after CREATE, found {self.curr.text}"
                )
        elif self.match(TokenType.DROP):
            if self.match(TokenType.TABLE):
                return self.parse_drop_table_statement()
            elif self.match(TokenType.INDEX):
                return self.parse_drop_index_statement()
            else:
                raise SyntaxError(
                    f"Expected TABLE or INDEX after DROP, found {self.curr.text}"
                )
        elif self.match(TokenType.INSERT):
            return self.parse_insert_statement()
        elif self.match(TokenType.SELECT):
            return self.parse_select_statement()
        elif self.match(TokenType.UPDATE):
            return self.parse_update_statement()
        elif self.match(TokenType.DELETE):
            return self.parse_delete_statement()
        else:
            raise SyntaxError(
                f"Expected statement keyword (CREATE, DROP, etc.), found {self.curr.text}"
            )

    def parse_statement_list(self) -> list[Statement]:
        self.print_debug("Parsing statement list")
        statement_list: list[Statement] = []

        if self.is_at_end():
            return statement_list
        statement = self.parse_statement()
        statement_list.append(statement)

        while self.match(TokenType.SEMICOLON) and not self.is_at_end():
            statement = self.parse_statement()
            statement_list.append(statement)
        return statement_list

    def parse_program(self) -> Program:
        try:
            statement_list = self.parse_statement_list()
            return Program(statement_list)
        except Exception as e:
            print(f"Error parsing program: {e}")
            return None


# endregion


def generate_random_inserts(num_records: int = 10) -> list[str]:
    fake = Faker()
    queries = []

    for i in range(num_records):
        # Generate random data
        user_id = i
        name = fake.name().replace("'", "''")  # Escape single quotes
        age = random.randint(18, 80)
        email = fake.email()

        query = f"INSERT INTO users (id, name, age, email) VALUES ({user_id}, '{name}', {age}, '{email}');"
        queries.append(query)

    return queries


if __name__ == "__main__":
    basic_creation_insertion_selection_test = [
        "CREATE TABLE student(id INT PRIMARY KEY, name VARCHAR(128), age INT, grade FLOAT)",
        "INSERT INTO student(name, grade, age, id) VALUES('Alice', 16.5, 19, 1)",
        "INSERT INTO student(id, age, grade, name) VALUES(2, 20, 12, 'Bob')",
        "INSERT INTO student(grade, id, age, name) VALUES(18, 3, 20, 'Charlie')",
        "INSERT INTO student(id, name, age, grade) VALUES(4, 'David', 21, 15.5)",
        "INSERT INTO student(id, name, age, grade) VALUES(5, 'Eve', 22, 17.0)",
        "SELECT * FROM student",
        "SELECT id, name FROM student",
        "SELECT id, name, grade FROM student WHERE age > 20",
        "SELECT id, name FROM student WHERE grade > 16",
        "DROP TABLE student",
    ]

    test_query_sets = [basic_creation_insertion_selection_test]

    printVisitor = PrintVisitor()
    runVisitor = RunVisitor()
    instruction_delay = 0.1

    for queryset in test_query_sets:
        for query in queryset:
            scanner = Scanner(query)
            # scanner.test()
            parser = Parser(scanner, debug=False)
            program = parser.parse_program()
            # printVisitor.visit_program(program)
            result: QueryResult = runVisitor.visit_program(program)
            if result.data is not None:
                print(f"Query Result: {result.data}")
            time.sleep(instruction_delay)
