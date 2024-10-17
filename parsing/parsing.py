import sqlglot
from sqlglot.expressions import (
    Create,
    ColumnDef,
    ForeignKey,
    PrimaryKey,
    Constraint,
    Check,
    Table,
    UniqueColumnConstraint,
    PrimaryKeyColumnConstraint,
    NotNullColumnConstraint,
    CheckColumnConstraint
)
import pprint


def parse_create_tables(sql_script):
    """
    Parses SQL CREATE TABLE statements and extracts table schema details,
    including columns, data types, constraints, and foreign keys.

    Args:
        sql_script (str): The SQL script containing CREATE TABLE statements.

    Returns:
        dict: A dictionary where each key is a table name and the value is
              another dictionary containing columns and foreign keys.
    """
    # Parse the SQL script with the appropriate dialect
    parsed = sqlglot.parse(sql_script, read='postgres')  # Adjust dialect if necessary
    tables = {}

    for statement in parsed:
        if isinstance(statement, Create):
            schema = statement.this
            if not isinstance(schema, sqlglot.expressions.Schema):
                continue  # Skip if not a Schema

            table = schema.this
            if not isinstance(table, Table):
                continue  # Skip if not a Table

            table_name = table.name
            columns = []
            table_foreign_keys = []
            table_unique_constraints = []
            table_primary_key = []
            table_checks = []

            # Debug: Print the table name
            print(f"Parsing table: {table_name}")

            for expression in schema.expressions:
                if isinstance(expression, ColumnDef):
                    col_name = expression.this.name
                    data_type = expression.args.get("kind").sql().upper()
                    constraints = expression.args.get("constraints", [])
                    column_info = {
                        "name": col_name,
                        "type": data_type,
                        "constraints": [],
                        "foreign_key": None
                    }

                    for constraint in constraints:
                        # Handle different constraint types based on their classes
                        if isinstance(constraint, PrimaryKeyColumnConstraint):
                            table_primary_key.append(col_name)
                            column_info["constraints"].append("PRIMARY KEY")
                        elif isinstance(constraint, UniqueColumnConstraint):
                            table_unique_constraints.append([col_name])
                            column_info["constraints"].append("UNIQUE")
                        elif isinstance(constraint, ForeignKey):
                            references = constraint.args.get("reference")
                            ref_table = references.this.name if references and references.this else None
                            ref_columns = [col.name for col in references.expressions] if references and references.expressions else []
                            column_info["foreign_key"] = {
                                "columns": [col_name],
                                "ref_table": ref_table,
                                "ref_columns": ref_columns
                            }
                            table_foreign_keys.append(column_info["foreign_key"])
                            column_info["constraints"].append(
                                f"FOREIGN KEY REFERENCES {ref_table}({', '.join(ref_columns)})"
                            )
                        elif isinstance(constraint, CheckColumnConstraint):
                            check_expression = constraint.args.get("this").sql()
                            table_checks.append(check_expression)
                            column_info["constraints"].append(f"CHECK ({check_expression})")
                        elif isinstance(constraint, Constraint):
                            # Handle unnamed constraints or other types
                            constraint_sql = constraint.sql().upper()
                            column_info["constraints"].append(constraint_sql)
                        else:
                            # Handle other constraint types if necessary
                            constraint_sql = constraint.sql().upper()
                            column_info["constraints"].append(constraint_sql)

                    columns.append(column_info)

                elif isinstance(expression, ForeignKey):
                    # Handle table-level foreign keys
                    fk_columns = [col.name for col in expression.expressions]
                    references = expression.args.get("reference")
                    ref_table = references.this.name if references and references.this else None
                    ref_columns = [col.name for col in references.expressions] if references and references.expressions else []
                    table_foreign_keys.append({
                        "columns": fk_columns,
                        "ref_table": ref_table,
                        "ref_columns": ref_columns
                    })
                    print(f"Added table-level foreign key: {fk_columns} -> {ref_table}({ref_columns})")

                elif isinstance(expression, PrimaryKey):
                    # Handle table-level primary keys
                    pk_columns = [col.name for col in expression.expressions]
                    table_primary_key.extend(pk_columns)
                    print(f"Added table-level primary key: {pk_columns}")

                elif isinstance(expression, Constraint):
                    # Handle table-level constraints
                    constraint_kind = expression.args.get("kind")
                    if isinstance(expression.expressions[0], UniqueColumnConstraint):
                        unique_columns = [col.name for col in expression.expressions[0].this.expressions]
                        table_unique_constraints.append(unique_columns)
                        print(f"Added table-level unique constraint: {unique_columns}")
                    elif isinstance(expression.expressions[0], ForeignKey):
                        fk_columns = [col.name for col in expression.expressions[0].expressions]
                        references = expression.expressions[0].args.get("reference")
                        ref_table = references.this.name if references and references.this else None
                        ref_columns = [col.name for col in references.expressions] if references and references.expressions else []
                        table_foreign_keys.append({
                            "columns": fk_columns,
                            "ref_table": ref_table,
                            "ref_columns": ref_columns
                        })
                        print(f"Added table-level foreign key: {fk_columns} -> {ref_table}({ref_columns})")
                    elif isinstance(expression.expressions[0], CheckColumnConstraint):
                        check_expression = expression.expressions[0].args.get("this").sql()
                        table_checks.append(check_expression)
                        print(f"Added table-level check constraint: {check_expression}")

                elif isinstance(expression, Check):
                    # Handle table-level check constraints
                    check_expression = expression.args.get("this").sql()
                    table_checks.append(check_expression)
                    print(f"Added table-level check constraint: {check_expression}")

            tables[table_name] = {
                "columns": columns,
                "foreign_keys": table_foreign_keys,
                "primary_key": table_primary_key,
                "unique_constraints": table_unique_constraints,
                "check_constraints": table_checks
            }

    return tables
