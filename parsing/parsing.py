import re
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


def parse_create_tables(sql_script, dialect='postgres'):
    """
        Parses SQL CREATE TABLE statements and extracts table schema details,
        including columns, data types, constraints, and foreign keys.

        Parameters
        ----------
        sql_script : str
            The SQL script containing CREATE TABLE statements.
        dialect : str, optional
            The SQL dialect to parse. Defaults to 'postgres'.

        Returns
        -------
        dict
            A dictionary where each key is a table name and the value is
            another dictionary containing columns, foreign keys, and other
            schema details.

        Example
        -------
        >>> from parsing.parsing import parse_create_tables
        >>> sql_script = '''
        ... CREATE TABLE Members (
        ...     member_id SERIAL PRIMARY KEY,
        ...     first_name VARCHAR(50) NOT NULL,
        ...     last_name VARCHAR(50) NOT NULL,
        ...     email VARCHAR(100) NOT NULL UNIQUE,
        ...     registration_date DATE NOT NULL,
        ...     CONSTRAINT chk_email_format CHECK (email ~ '^[\\w\\.-]+@[\\w\\.-]+\\.\\w{2,}$')
        ... );
        ... '''
        >>> tables = parse_create_tables(sql_script)
        >>> print(tables)
        {
            'Members': {
                'columns': [
                    {
                        'name': 'member_id',
                        'type': 'SERIAL',
                        'constraints': ['PRIMARY KEY'],
                        'foreign_key': None,
                        'is_serial': True,
                    },
                    {
                        'name': 'first_name',
                        'type': 'VARCHAR(50)',
                        'constraints': ['NOT NULL'],
                        'foreign_key': None,
                        'is_serial': False,
                    },
                    ...
                ],
                'foreign_keys': [],
                'primary_key': ['member_id'],
                'unique_constraints': [['member_id'], ['email']],
                'check_constraints': [
                    "REGEXP_LIKE(email, '^[\\w\\.-]+@[\\w\\.-]+\\.\\w{2,}$')"
                ]
            }
        }
    """

    # Parse the SQL script with the appropriate dialect (e.g., 'postgres' or 'mysql')
    parsed = sqlglot.parse(sql_script, read=dialect)
    tables = {}

    for statement in parsed:
        if isinstance(statement, Create):
            schema = statement.this
            # If the CREATE statement doesn't have a proper Schema child, skip
            if not isinstance(schema, sqlglot.expressions.Schema):
                continue

            table_expr = schema.this
            if not isinstance(table_expr, Table):
                continue  # Not a table, skip it

            table_name = table_expr.name
            columns = []
            table_foreign_keys = []
            table_unique_constraints = []
            table_primary_key = []
            table_checks = []

            for expression in schema.expressions:
                # ─────────────────────────────────────────────────────────────
                # 1) COLUMN DEFINITIONS
                # ─────────────────────────────────────────────────────────────
                if isinstance(expression, ColumnDef):
                    col_name = expression.this.name
                    data_type = expression.args.get("kind").sql().upper()  # e.g. "INT UNSIGNED", "SERIAL"
                    constraints = expression.args.get("constraints", [])

                    # Prepare column_info dictionary
                    column_info = {
                        "name": col_name,
                        "type": data_type,
                        "constraints": [],
                        "foreign_key": None,
                        "is_serial": False,  # Will set True if we detect SERIAL or AUTO_INCREMENT
                    }

                    # Check if data_type itself is 'SERIAL'
                    # (or might contain it, e.g. "BIGSERIAL")
                    if "SERIAL" in data_type:
                        column_info["is_serial"] = True

                    # Process each column-level constraint
                    for constraint in constraints:
                        # PRIMARY KEY constraint
                        if isinstance(constraint.kind, PrimaryKeyColumnConstraint):
                            table_primary_key.append(col_name)
                            column_info["constraints"].append("PRIMARY KEY")
                            table_unique_constraints.append([col_name])

                        # UNIQUE constraint
                        elif isinstance(constraint.kind, UniqueColumnConstraint):
                            table_unique_constraints.append([col_name])
                            column_info["constraints"].append("UNIQUE")

                        # FOREIGN KEY constraint
                        elif isinstance(constraint.kind, ForeignKey):
                            references = constraint.args.get("reference")
                            if references:
                                if isinstance(references.this, Table):
                                    ref_table = references.this.name
                                elif isinstance(references.this, sqlglot.expressions.Schema):
                                    ref_table = references.this.this.name
                                else:
                                    ref_table = None
                                ref_columns = (
                                    [col.name for col in references.this.expressions]
                                    if references.this and references.this.expressions
                                    else []
                                )
                            else:
                                ref_table = None
                                ref_columns = []

                            column_info["foreign_key"] = {
                                "columns": [col_name],
                                "ref_table": ref_table,
                                "ref_columns": ref_columns
                            }
                            table_foreign_keys.append(column_info["foreign_key"])
                            column_info["constraints"].append(
                                f"FOREIGN KEY REFERENCES {ref_table}({', '.join(ref_columns)})"
                            )

                        # CHECK constraint
                        elif isinstance(constraint.kind, CheckColumnConstraint):
                            check_expression = constraint.args.get("this").sql()
                            table_checks.append(check_expression)
                            column_info["constraints"].append(f"CHECK ({check_expression})")

                        # NOT NULL constraint
                        elif isinstance(constraint.kind, NotNullColumnConstraint):
                            column_info["constraints"].append("NOT NULL")

                        else:
                            # Other constraint types or direct SQL
                            constraint_sql = constraint.sql().upper()
                            column_info["constraints"].append(constraint_sql)

                            # If we see AUTO_INCREMENT in the constraint,
                            # mark is_serial = True
                            if "AUTO_INCREMENT" in constraint_sql:
                                column_info["is_serial"] = True

                    columns.append(column_info)

                # ─────────────────────────────────────────────────────────────
                # 2) TABLE-LEVEL FOREIGN KEY
                # ─────────────────────────────────────────────────────────────
                elif isinstance(expression, ForeignKey):
                    fk_columns = [col.name for col in expression.expressions]
                    references = expression.args.get("reference")
                    if references:
                        if isinstance(references.this, Table):
                            ref_table = references.this.name
                        elif isinstance(references.this, sqlglot.expressions.Schema):
                            ref_table = references.this.this.name
                        else:
                            ref_table = None
                        ref_columns = (
                            [col.name for col in references.this.expressions]
                            if references.this and references.this.expressions
                            else []
                        )
                    else:
                        ref_table = None
                        ref_columns = []

                    table_foreign_keys.append({
                        "columns": fk_columns,
                        "ref_table": ref_table,
                        "ref_columns": ref_columns
                    })

                # ─────────────────────────────────────────────────────────────
                # 3) TABLE-LEVEL PRIMARY KEY
                # ─────────────────────────────────────────────────────────────
                elif isinstance(expression, PrimaryKey):
                    # e.g. PRIMARY KEY (col1, col2)
                    pk_columns = [col.name for col in expression.expressions]
                    table_primary_key.extend(pk_columns)
                    table_unique_constraints.append(pk_columns)

                # ─────────────────────────────────────────────────────────────
                # 4) TABLE-LEVEL CONSTRAINT (UNIQUE, PK, FK, CHECK, etc.)
                # ─────────────────────────────────────────────────────────────
                elif isinstance(expression, Constraint):
                    if not expression.expressions:
                        continue
                    first_expr = expression.expressions[0]

                    # UNIQUE constraint
                    if isinstance(first_expr, UniqueColumnConstraint):
                        unique_columns = [col.name for col in first_expr.this.expressions]
                        table_unique_constraints.append(unique_columns)

                    # PRIMARY KEY constraint
                    elif isinstance(first_expr, PrimaryKey):
                        pk_columns = [col.name for col in first_expr.expressions]
                        table_primary_key.extend(pk_columns)
                        table_unique_constraints.append(pk_columns)

                    # FOREIGN KEY constraint
                    elif isinstance(first_expr, ForeignKey):
                        fk_columns = [col.name for col in first_expr.expressions]
                        references = first_expr.args.get("reference")
                        if references:
                            if isinstance(references.this, Table):
                                ref_table = references.this.name
                            elif isinstance(references.this, sqlglot.expressions.Schema):
                                ref_table = references.this.this.name
                            else:
                                ref_table = None
                            ref_columns = (
                                [col.name for col in references.this.expressions]
                                if references.this and references.this.expressions
                                else []
                            )
                        else:
                            ref_table = None
                            ref_columns = []

                        table_foreign_keys.append({
                            "columns": fk_columns,
                            "ref_table": ref_table,
                            "ref_columns": ref_columns
                        })

                    # CHECK constraint
                    elif isinstance(first_expr, CheckColumnConstraint):
                        check_expression = first_expr.args.get("this").sql()
                        table_checks.append(check_expression)

                # ─────────────────────────────────────────────────────────────
                # 5) TABLE-LEVEL CHECK
                # ─────────────────────────────────────────────────────────────
                elif isinstance(expression, Check):
                    check_expression = expression.args.get("this").sql()
                    table_checks.append(check_expression)

            # Finally, assemble the table metadata
            tables[table_name] = {
                "columns": columns,
                "foreign_keys": table_foreign_keys,
                "primary_key": table_primary_key,
                "unique_constraints": table_unique_constraints,
                "check_constraints": table_checks
            }

    return tables
