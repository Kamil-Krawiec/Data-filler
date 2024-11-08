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
        ----
            sql_script (str): The SQL script containing CREATE TABLE statements.
            dialect (str, optional): The SQL dialect to parse. Defaults to 'postgres'.

        Returns
        -------
            dict: A dictionary where each key is a table name and the value is
                  another dictionary containing columns, foreign keys, and other
                  schema details.

        Example
        -------
            >>> from parsing.parsing import parse_create_tables
            >>> sql_script =
            '''
            CREATE TABLE Members (
            member_id SERIAL PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(100) NOT NULL UNIQUE,
            registration_date DATE NOT NULL,
            CONSTRAINT chk_email_format CHECK (email ~ '^[\\w\\.-]+@[\\w\\.-]+\\.\\w{2,}$'));'''
            >>> tables = parse_create_tables(sql_script)
            >>> print(tables)
        Output
        ------
        {

        'Members': {
            'columns': [
                {'name': 'member_id', 'type': 'SERIAL', 'constraints': ['PRIMARY KEY'], 'foreign_key': None},

                {'name': 'first_name', 'type': 'VARCHAR(50)', 'constraints': ['NOT NULL'], 'foreign_key': None},

                {'name': 'last_name', 'type': 'VARCHAR(50)', 'constraints': ['NOT NULL'], 'foreign_key': None},

                {'name': 'email', 'type': 'VARCHAR(100)', 'constraints': ['NOT NULL', 'UNIQUE'], 'foreign_key': None},

                {'name': 'registration_date', 'type': 'DATE', 'constraints': ['NOT NULL'], 'foreign_key': None}

            ],
            'foreign_keys': [],

            'primary_key': ['member_id'],

            'unique_constraints': [['member_id'], ['email']],

            'check_constraints': ["REGEXP_LIKE(email, '^[\\w\\.-]+@[\\w\\.-]+\\.\\w{2,}$')"]
        }}
    """

    # Parse the SQL script with the appropriate dialect here (e.g., 'postgres')
    parsed = sqlglot.parse(sql_script, read=dialect)
    tables = {}

    for statement in parsed:
        if isinstance(statement, Create):
            schema = statement.this
            if not isinstance(schema, sqlglot.expressions.Schema):
                continue  # Skip if not a Schema

            table = schema.this
            if not isinstance(table, Table):
                continue  # Not a table so skip

            table_name = table.name
            columns = []
            table_foreign_keys = []
            table_unique_constraints = []
            table_primary_key = []
            table_checks = []

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
                        if isinstance(constraint.kind, PrimaryKeyColumnConstraint):
                            table_primary_key.append(col_name)
                            column_info["constraints"].append("PRIMARY KEY")
                            table_unique_constraints.append([col_name])  # Added line
                        elif isinstance(constraint.kind, UniqueColumnConstraint):
                            table_unique_constraints.append([col_name])
                            column_info["constraints"].append("UNIQUE")
                        elif isinstance(constraint.kind, ForeignKey):
                            references = constraint.args.get("reference")
                            if references:
                                if isinstance(references.this, Table):
                                    ref_table = references.this.name
                                elif isinstance(references.this, sqlglot.expressions.Schema):
                                    ref_table = references.this.this.name
                                else:
                                    ref_table = None
                                ref_columns = [col.name for col in
                                               references.this.expressions] if references.this and references.this.expressions else []
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
                        elif isinstance(constraint.kind, CheckColumnConstraint):
                            check_expression = constraint.args.get("this").sql()
                            table_checks.append(check_expression)
                            column_info["constraints"].append(f"CHECK ({check_expression})")
                        elif isinstance(constraint.kind, NotNullColumnConstraint):
                            column_info["constraints"].append("NOT NULL")
                        else:
                            # Handle other constraint types if necessary
                            constraint_sql = constraint.sql().upper()
                            column_info["constraints"].append(constraint_sql)

                    columns.append(column_info)

                elif isinstance(expression, ForeignKey):
                    # Handle table-level foreign keys
                    fk_columns = [col.name for col in expression.expressions]
                    references = expression.args.get("reference")
                    if references:
                        if isinstance(references.this, Table):
                            ref_table = references.this.name
                        elif isinstance(references.this, sqlglot.expressions.Schema):
                            ref_table = references.this.this.name
                        else:
                            ref_table = None
                        ref_columns = [col.name for col in
                                       references.this.expressions] if references.this and references.this.expressions else []
                    else:
                        ref_table = None
                        ref_columns = []
                    table_foreign_keys.append({
                        "columns": fk_columns,
                        "ref_table": ref_table,
                        "ref_columns": ref_columns
                    })

                elif isinstance(expression, PrimaryKey):
                    # Handle table-level primary keys
                    pk_columns = [col.name for col in expression.expressions]
                    table_primary_key.extend(pk_columns)
                    table_unique_constraints.append(pk_columns)  # Added line

                elif isinstance(expression, Constraint):
                    # Handle table-level constraints
                    if not expression.expressions:
                        continue
                    first_expr = expression.expressions[0]
                    if isinstance(first_expr, UniqueColumnConstraint):
                        unique_columns = [col.name for col in first_expr.this.expressions]
                        table_unique_constraints.append(unique_columns)
                    elif isinstance(first_expr, PrimaryKey):
                        # Handle PRIMARY KEY constraints
                        pk_columns = [col.name for col in first_expr.expressions]
                        table_primary_key.extend(pk_columns)
                        table_unique_constraints.append(pk_columns)  # Added line
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
                            ref_columns = [col.name for col in
                                           references.this.expressions] if references.this and references.this.expressions else []
                        else:
                            ref_table = None
                            ref_columns = []
                        table_foreign_keys.append({
                            "columns": fk_columns,
                            "ref_table": ref_table,
                            "ref_columns": ref_columns
                        })
                    elif isinstance(first_expr, CheckColumnConstraint):
                        check_expression = first_expr.args.get("this").sql()
                        table_checks.append(check_expression)

                elif isinstance(expression, Check):
                    # Handle table-level check constraints
                    check_expression = expression.args.get("this").sql()
                    table_checks.append(check_expression)

            tables[table_name] = {
                "columns": columns,
                "foreign_keys": table_foreign_keys,
                "primary_key": table_primary_key,
                "unique_constraints": table_unique_constraints,
                "check_constraints": table_checks
            }

    return tables
