from contextlib import contextmanager
from column_types import ColumnType, QueryResult, IndexType, OperationType

from storage.HeapFile import HeapFile
from storage.Record import Record

import os

from fancytypes.schema import SchemaType

from database import (
    create_table,
    create_seq_idx,
    create_btree_idx,
    create_rtree_idx,
    create_hash_idx,
    check_table_exists,
    drop_table,
    get_table_schema,
    check_seq_idx,
    check_btree_idx,
    check_hash_idx,
    check_rtree_idx,
    drop_seq_idx,
    drop_btree_idx,
    drop_hash_idx,
    drop_rtree_idx,
    insert_record,
    _table_path,
)

from statement import (
    CreateTableStatement,
    CreateIndexStatement,
    InsertStatement,
    DropIndexStatement,
    DropTableStatement,
    IntExpression,
    FloatExpression,
    StringExpression,
    BoolExpression,
    ColumnExpression,
    SelectStatement,
    WhereStatement,
    Program,
    Point2DExpression,
    Point3DExpression,
    # xd,
    OrCondition,
    AndCondition,
    NotCondition,
    PrimaryCondition,
    ConstantCondition,
    SimpleComparison,
    BetweenComparison,
)


def fmt_to_column_type(fmt: str) -> ColumnType:
    match fmt:
        case "i":
            return ColumnType.INT
        case "f":
            return ColumnType.FLOAT
        case "?":
            return ColumnType.BOOL
        case "2f":
            return ColumnType.POINT2D
        case "3f":
            return ColumnType.POINT3D
        case _:
            pass  # TODO: implement dates
    if fmt.endswith("s"):
        return ColumnType.VARCHAR
    raise ValueError(f"Unsupported format: {fmt}")


def column_type_to_fmt(column_type: ColumnType, varchar_length: int) -> str:
    match column_type:
        case ColumnType.INT:
            return "i"
        case ColumnType.FLOAT:
            return "f"
        case ColumnType.VARCHAR:
            return f"{varchar_length}s"
        case ColumnType.BOOL:
            return "?"
        case ColumnType.POINT2D:
            return "2f"
        case ColumnType.POINT3D:
            return "3f"
        case _:
            raise ValueError(f"Unsupported column type: {column_type}")


class Visitor:
    """Base visitor class with default behavior"""

    def generic_visit(self, node):
        """Called if no explicit visitor function exists for a node."""
        print("GENERIC VISIT CALLED ON ", node.__class__.__name__)
        pass


# region RunVisitor
class RunVisitor:
    """Base visitor class for executing statements"""

    def __init__(self):
        self.current_record: Record = None

    def generic_visit(self, node):
        """Called if no explicit visitor function exists for a node."""
        print("GENERIC VISIT CALLED ON ", node.__class__.__name__)
        pass

    def visit_intexpression(self, expr: IntExpression):
        return expr.value

    def visit_floatexpression(self, expr: FloatExpression):
        return expr.value

    def visit_stringexpression(self, expr: StringExpression):
        return expr.value

    def visit_boolexpression(self, expr: BoolExpression):
        return expr.value

    def visit_point2dexpression(self, expr: Point2DExpression):
        return (expr.x, expr.y)

    def visit_point3dexpression(self, expr: Point3DExpression):
        return (expr.x, expr.y, expr.z)

    def visit_columnexpression(self, expr: ColumnExpression):
        # resolves colExp yeehaw
        if self.current_record is None:
            raise ValueError("Current record is not set.")
        if expr.table_name and expr.table_name != os.path.basename(
            self.current_record.schema[0][0]
        ):
            raise ValueError(
                f"Table '{expr.table_name}' does not match the current record's table."
            )
        schema_names = [name for name, _ in self.current_record.schema]
        if expr.column_name not in schema_names:
            raise ValueError(
                f"Column '{expr.column_name}' does not exist in the current record."
            )
        idx = schema_names.index(expr.column_name)
        return self.current_record.values[idx]

    def visit_program(self, program: Program):
        lastResult: QueryResult = None
        for st in program.statement_list:
            lastResult = st.accept(self)
            print(lastResult.message)
        return lastResult

    def visit_createtablestatement(self, st: CreateTableStatement):
        if check_table_exists(st.table_name):
            raise ValueError(f"Table '{st.table_name}' already exists.")
        pk: str = None
        schema: SchemaType = []
        for col in st.columns:
            fmt: str = column_type_to_fmt(col.column_type, col.varchar_length)
            if col.is_pk:
                if pk is not None:
                    raise ValueError("Multiple primary keys are not allowed.")
                pk = col.column_name
            schema.append((col.column_name, fmt))

        create_table(st.table_name, schema, pk)
        return QueryResult(True, f"Table '{st.table_name}' created successfully.")

    def visit_droptablestatement(self, st: DropTableStatement):
        if not check_table_exists(st.table_name):
            raise ValueError(f"Table '{st.table_name}' does not exist.")
        drop_table(st.table_name)
        return QueryResult(True, f"Table '{st.table_name}' dropped successfully.")

    def visit_createindexstatement(self, st: CreateIndexStatement):
        schema: SchemaType = get_table_schema(
            st.table_name
        )  # also checks if table exists
        fmt: str = None

        for name, format in schema:  # WHY IS SCHEMA A LIST
            if st.column_name == name:
                fmt = format
                break

        if fmt is None:
            raise ValueError(
                f"Column '{st.column_name}' does not exist in table '{st.table_name}'."
            )

        actual_type = fmt_to_column_type(fmt)

        if st.index_type == IndexType.BPLUSTREE:
            if actual_type not in (
                ColumnType.INT,
                ColumnType.FLOAT,
                ColumnType.VARCHAR,
            ):
                raise ValueError(
                    f"B+ Tree index can only be created on INT, FLOAT or VARCHAR columns, not {actual_type}."
                )
            create_btree_idx(st.table_name, st.column_name)
        elif st.index_type == IndexType.EXTENDIBLEHASH:
            if actual_type not in (ColumnType.INT, ColumnType.VARCHAR):
                raise ValueError(
                    f"Extendible Hash index can only be created on INT or VARCHAR columns, not {actual_type}."
                )
            create_hash_idx(st.table_name, st.column_name)
        elif st.index_type == IndexType.RTREE:
            if actual_type not in (ColumnType.POINT2D, ColumnType.POINT3D):
                raise ValueError(
                    f"R-Tree index can only be created on POINT2D or POINT3D columns, not {actual_type}."
                )
            create_rtree_idx(st.table_name, st.column_name)
        elif st.index_type == IndexType.SEQUENTIAL:
            if actual_type not in (
                ColumnType.INT,
                ColumnType.FLOAT,
                ColumnType.VARCHAR,
            ):
                raise ValueError(
                    f"Sequential index can only be created on INT, FLOAT or VARCHAR columns, not {actual_type}."
                )
            create_seq_idx(st.table_name, st.column_name)
        else:
            raise ValueError(f"Unsupported index type: {st.index_type}")

        return QueryResult(
            True,
            f"Index {st.index_type} on column '{st.column_name}' in table '{st.table_name}' created successfully.",
        )

    def visit_dropindexstatement(self, st: DropIndexStatement):
        if st.index_type == IndexType.BPLUSTREE:
            if not check_btree_idx(st.table_name, st.column_name):
                raise ValueError(
                    f"B+ Tree index on column '{st.column_name}' in table '{st.table_name}' does not exist."
                )
            drop_btree_idx(st.table_name, st.column_name)
        elif st.index_type == IndexType.EXTENDIBLEHASH:
            if not check_hash_idx(st.table_name, st.column_name):
                raise ValueError(
                    f"Extendible Hash index on column '{st.column_name}' in table '{st.table_name}' does not exist."
                )
            drop_hash_idx(st.table_name, st.column_name)
        elif st.index_type == IndexType.RTREE:
            if not check_rtree_idx(st.table_name, st.column_name):
                raise ValueError(
                    f"R-Tree index on column '{st.column_name}' in table '{st.table_name}' does not exist."
                )
            drop_rtree_idx(st.table_name, st.column_name)
        elif st.index_type == IndexType.SEQUENTIAL:
            if not check_seq_idx(st.table_name, st.column_name):
                raise ValueError(
                    f"Sequential index on column '{st.column_name}' in table '{st.table_name}' does not exist."
                )
            drop_seq_idx(st.table_name, st.column_name)
        else:
            raise ValueError(f"Unsupported index type: {st.index_type}")

        return QueryResult(
            True,
            f"Index {st.index_type} on column '{st.column_name}' in table '{st.table_name}' dropped successfully.",
        )

    def visit_insertstatement(self, st: InsertStatement):
        if not check_table_exists(st.table_name):
            raise ValueError(f"Table '{st.table_name}' does not exist.")

        if len(st.column_names) != len(st.values):
            raise ValueError(
                f"Number of column names ({len(st.column_names)}) does not match number of values ({len(st.values)})."
            )

        # now we gotta check that types match the table schema
        # we must also follow the order of the column names
        # :(

        schema: SchemaType = get_table_schema(st.table_name)
        schema_dict = {name: fmt for name, fmt in schema}

        for col_name, const_exp in zip(st.column_names, st.values):
            if col_name not in schema_dict:
                raise ValueError(
                    f"Column '{col_name}' does not exist in table '{st.table_name}'."
                )

            fmt = schema_dict[col_name]
            actual_type = fmt_to_column_type(fmt)

            if actual_type == ColumnType.INT and not isinstance(
                const_exp, IntExpression
            ):
                raise ValueError(f"Value for column '{col_name}' must be of type INT.")
            elif actual_type == ColumnType.FLOAT and not isinstance(
                const_exp, (IntExpression | FloatExpression)
            ):
                raise ValueError(
                    f"Value for column '{col_name}' must be of type FLOAT or INT."
                )
            elif actual_type == ColumnType.VARCHAR and not isinstance(
                const_exp, StringExpression
            ):
                raise ValueError(
                    f"Value for column '{col_name}' must be of type VARCHAR."
                )
            elif actual_type == ColumnType.BOOL and not isinstance(
                const_exp, BoolExpression
            ):
                raise ValueError(f"Value for column '{col_name}' must be of type BOOL.")
            elif actual_type == ColumnType.POINT2D and not isinstance(
                const_exp, Point2DExpression
            ):
                raise ValueError(
                    f"Value for column '{col_name}' must be of type POINT2D."
                )
            elif actual_type == ColumnType.POINT3D and not isinstance(
                const_exp, Point3DExpression
            ):
                raise ValueError(
                    f"Value for column '{col_name}' must be of type POINT3D."
                )

            # else: it's fine, we can insert it
            # but first we check for length of VARCHAR

            if actual_type == ColumnType.VARCHAR:
                if len(const_exp.value) > int(schema_dict[col_name].split("s")[0]):
                    raise ValueError(
                        f"Value for column '{col_name}' exceeds maximum length of {schema_dict[col_name].split('s')[0]} characters."
                    )
        # all values are valid, but we still need to convert it into a record and give
        # it the values in the right order

        value_dict = dict(zip(st.column_names, st.values), strict=True)
        record_values = []
        for name, fmt in schema:
            if name in value_dict:
                record_values.append(
                    value_dict[name].value
                )  # TODO: correctly pass point2d or point3d values as tuple
            else:
                raise ValueError(
                    f"Column '{name}' is missing in the insert statement. NULL is not supported yet."
                )
        record = Record(schema, record_values)
        insert_record(st.table_name, record)
        return QueryResult(
            True,
            f"Record inserted into table '{st.table_name}' successfully.",
        )

    def visit_selectstatement(self, st: SelectStatement):
        if not check_table_exists(st.from_table):
            raise ValueError(f"Table '{st.from_table}' does not exist.")
        heapfile = HeapFile(_table_path(st.from_table))
        records = heapfile.get_all_records()
        table_name = os.path.basename(heapfile.table_name)
        result = []
        n, c = None, None

        for rec in records:
            self.current_record = rec
            if st.where_statement is not None:
                if not st.where_statement.accept(self):
                    continue
            entry = []
            if st.select_all:
                entry = rec.values
            else:
                for col in st.select_columns:
                    col_name = col
                    if "." in col:
                        n, c = col.split(".")
                        col_name = c
                        if n != table_name:
                            raise ValueError(
                                f"Table '{n}' does not match the table in the statement '{st.from_table}'."
                            )

                    if hasattr(rec, "schema") and hasattr(rec, "values"):
                        schema_names = [name for name, _ in rec.schema]

                        if col_name not in schema_names:
                            raise ValueError(
                                f"Column '{col_name}' does not exist in table '{st.from_table}'."
                            )
                        idx = schema_names.index(col_name)
                        entry.append(rec.values[idx])
                    else:
                        raise ValueError(
                            "Record does not have a schema or values attribute???"
                        )
            result.append(entry)
        if st.limit is not None:
            if st.limit < 0:
                raise ValueError("Limit cannot be negative.")
            result = result[: st.limit]
        return QueryResult(
            True,
            f"Selected {len(result)} records from table '{st.from_table}'.",
            result,
        )  # result is an array of records

    # region RunVisitor Conditions
    def visit_orcondition(self, condition: OrCondition):
        if condition.or_condition is not None:
            return condition.and_condition.accept(
                self
            ) or condition.or_condition.accept(self)
        else:
            return condition.and_condition.accept(self)

    def visit_andcondition(self, condition: AndCondition):
        if condition.and_condition is not None:
            return condition.not_condition.accept(
                self
            ) and condition.and_condition.accept(self)
        else:
            return condition.not_condition.accept(self)

    def visit_notcondition(self, condition: NotCondition):
        if condition.is_not:
            return not condition.primary_condition.accept(self)
        else:
            return condition.primary_condition.accept(self)

    def visit_constantcondition(self, condition: ConstantCondition):
        return condition.bool_constant.accept(self)

    def visit_simplecomparison(self, condition: SimpleComparison):
        left_value = condition.left_expression.accept(self)
        right_value = condition.right_expression.accept(self)

        match condition.operator:
            case OperationType.EQUAL:
                return left_value == right_value
            case OperationType.NOT_EQUAL:
                return left_value != right_value
            case OperationType.LESS_THAN:
                return left_value < right_value
            case OperationType.LESS__EQUAL:
                return left_value <= right_value
            case OperationType.GREATER_THAN:
                return left_value > right_value
            case OperationType.GREATER__EQUAL:
                return left_value >= right_value
            case _:
                raise ValueError(f"Unsupported operator: {condition.operator}")

    def visit_betweencomparison(self, condition: BetweenComparison):
        left_value = condition.left_expression.accept(self)
        lower_bound = condition.lower_bound.accept(self)
        upper_bound = condition.upper_bound.accept(self)

        return lower_bound <= left_value <= upper_bound

    def visit_primarycondition(self, condition: PrimaryCondition):
        if isinstance(condition.condition, ConstantCondition):
            return condition.condition.accept(self)
        elif isinstance(condition.condition, SimpleComparison):
            return condition.condition.accept(self)
        elif isinstance(condition.condition, BetweenComparison):
            return condition.condition.accept(self)
        elif isinstance(condition.condition, OrCondition):
            return condition.condition.accept(self)  # nested OrCondition
        else:
            raise ValueError(f"Unsupported condition type: {type(condition.condition)}")

    # endregion

    def visit_wherestatement(self, st: WhereStatement):
        return st.or_condition.accept(self)  # returns bool, not print


# endregion


# region PrintVisitor


class PrintVisitor(Visitor):
    def __init__(self, indent_size: int = 2):
        self.indent_level = 0
        self.indent_size = indent_size

    @contextmanager
    def indented(self):
        self.indent_level += 1
        try:
            yield
        finally:
            self.indent_level -= 1

    def print_line(self, text: str, end: str = "\n"):
        indent = " " * (self.indent_level * self.indent_size)
        print(f"{indent}{text}", end=end)

    def visit_intexpression(self, expr: IntExpression):
        self.print_line(f"{expr.value}", "")

    def visit_floatexpression(self, expr: FloatExpression):
        self.print_line(f"{expr.value}", "")

    def visit_stringexpression(self, expr: StringExpression):
        self.print_line(f"'{expr.value}'", "")

    def visit_boolexpression(self, expr: BoolExpression):
        self.print_line("TRUE" if expr.value else "FALSE", "")

    def visit_point2dexpression(self, expr: Point2DExpression):
        self.print_line(f"POINT2D({expr.x}, {expr.y})", "")

    def visit_point3dexpression(self, expr: Point3DExpression):
        self.print_line(f"POINT3D({expr.x}, {expr.y}, {expr.z})", "")

    def visit_columnexpression(self, expr: ColumnExpression):
        if expr.table_name:
            self.print_line(f"{expr.table_name}.{expr.column_name}", "")
        else:
            self.print_line(expr.column_name, "")

    def visit_program(self, program: Program):
        for st in program.statement_list:
            st.accept(self)

    def visit_createtablestatement(self, st: CreateTableStatement):
        self.print_line(f"CREATE TABLE {st.table_name}(")
        with self.indented():
            for column in st.columns:
                column_def = f"{column.column_name} {column.column_type}{' PRIMARY KEY' if column.is_pk else ''}"
                if column.column_type == ColumnType.VARCHAR:
                    column_def += f"({column.varchar_length})"
                self.print_line(
                    f"{column_def}{',' if column != st.columns[-1] else ''}"
                )
        self.print_line(");")

    def visit_droptablestatement(self, statement: DropTableStatement):
        self.print_line(f"DROP TABLE {statement.table_name}")

    def visit_createindexstatement(self, st: CreateIndexStatement):
        self.print_line(
            f"CREATE INDEX ON {st.table_name}({st.column_name}) USING {st.index_type};"
        )

    def visit_dropindexstatement(self, st: DropIndexStatement):
        self.print_line(
            f"DROP INDEX {st.index_type} ON {st.table_name}({st.column_name});"
        )

    def visit_insertstatement(self, st: InsertStatement):
        self.print_line(f"INSERT INTO {st.table_name} VALUES(")
        for value in st.values:
            with self.indented():
                value.accept(self)  # intexpression, floatexpression, stringexpression
            self.print_line(f"{',' if value != st.values[-1] else ''}")
        self.print_line(");")

    def visit_selectstatement(self, st: SelectStatement):
        self.print_line(
            f"SELECT {', '.join(st.select_columns)} FROM {st.from_table}", ""
        )
        if st.where_statement:
            st.where_statement.accept(self)
        if st.limit is not None:
            self.print_line(f" LIMIT {st.limit}", "")
        self.print_line(";")

    # region PrintVisitor Conditions
    def visit_orcondition(self, condition: OrCondition):
        condition.and_condition.accept(self)
        if condition.or_condition is not None:
            self.print_line(" OR ", "")
            condition.or_condition.accept(self)

    def visit_andcondition(self, condition: AndCondition):
        condition.not_condition.accept(self)
        if condition.and_condition is not None:
            self.print_line(" AND ", "")
            condition.and_condition.accept(self)

    def visit_notcondition(self, condition: NotCondition):
        if condition.is_not:
            self.print_line(" NOT ", "")
        condition.primary_condition.accept(self)

    def visit_constantcondition(self, condition: ConstantCondition):
        condition.bool_constant.accept(self)

    def visit_simplecomparison(self, condition: SimpleComparison):
        condition.left_expression.accept(self)
        self.print_line(f" {condition.operator} ", "")
        condition.right_expression.accept(self)

    def visit_betweencomparison(self, condition: BetweenComparison):
        condition.left_expression.accept(self)
        self.print_line(" BETWEEN ", "")
        condition.lower_bound.accept(self)
        self.print_line(" AND ", "")
        condition.upper_bound.accept(self)

    def visit_primarycondition(self, condition: PrimaryCondition):
        if isinstance(condition.condition, ConstantCondition):
            condition.condition.accept(self)
        elif isinstance(condition.condition, SimpleComparison):
            condition.condition.accept(self)
        elif isinstance(condition.condition, BetweenComparison):
            condition.condition.accept(self)
        elif isinstance(condition.condition, OrCondition):
            self.print_line("(", "")  # cause nested
            condition.condition.accept(self)
            self.print_line(")", "")
        else:
            raise ValueError(f"Unsupported condition type: {type(condition.condition)}")

    # endregion

    def visit_wherestatement(self, st: WhereStatement):
        self.print_line(" WHERE ", "")
        st.or_condition.accept(self)


# endregion
