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
import logging
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def _extract_reference(references: Any) -> Tuple[Optional[str], List[str]]:
    """
    Extract reference table name and column names from a foreign key reference.
    """
    if references:
        if isinstance(references.this, Table):
            ref_table = references.this.name
        elif hasattr(references.this, "this"):
            ref_table = references.this.this.name
        else:
            ref_table = None
        ref_columns = (
            [col.name for col in references.this.expressions]
            if references.this and getattr(references.this, "expressions", None)
            else []
        )
        return ref_table, ref_columns
    return None, []


def _parse_column_def(expression: ColumnDef) -> Dict[str, Any]:
    """
    Parse a column definition and return its metadata.
    """
    col_name = getattr(expression.this, "name", "").strip()
    if not col_name:
        logger.warning("Column definition missing name; will assign default later.")
    data_type = expression.args.get("kind").sql().upper() if expression.args.get("kind") else ""
    constraints = expression.args.get("constraints", [])

    column_info = {
        "name": col_name,
        "type": data_type,
        "constraints": [],
        "foreign_key": None,
        "is_serial": "SERIAL" in data_type,
    }

    for constraint in constraints:
        if isinstance(constraint.kind, PrimaryKeyColumnConstraint):
            column_info["constraints"].append("PRIMARY KEY")
        elif isinstance(constraint.kind, UniqueColumnConstraint):
            column_info["constraints"].append("UNIQUE")
        elif isinstance(constraint.kind, ForeignKey):
            ref_table, ref_columns = _extract_reference(constraint.args.get("reference"))
            fk_info = {
                "columns": [col_name],
                "ref_table": ref_table,
                "ref_columns": ref_columns
            }
            on_delete = constraint.args.get("on_delete")
            if on_delete:
                fk_info["on_delete"] = on_delete.sql() if hasattr(on_delete, "sql") else str(on_delete)
            column_info["foreign_key"] = fk_info
            constraint_text = f"FOREIGN KEY REFERENCES {ref_table}({', '.join(ref_columns)})"
            if on_delete:
                constraint_text += f" ON DELETE {fk_info['on_delete']}"
            column_info["constraints"].append(constraint_text)
        elif isinstance(constraint.kind, CheckColumnConstraint):
            check_expression = constraint.args.get("this")
            if check_expression:
                expr_sql = check_expression.sql()
                column_info["constraints"].append(f"CHECK ({expr_sql})")
            else:
                raw_sql = constraint.sql()
                match = re.search(r'CHECK\s*\((.+)\)', raw_sql, re.IGNORECASE)
                if match:
                    extracted_expr = match.group(1).strip()
                    column_info["constraints"].append(f"CHECK ({extracted_expr})")
        elif isinstance(constraint.kind, NotNullColumnConstraint):
            column_info["constraints"].append("NOT NULL")
        else:
            constraint_sql = constraint.sql().upper()
            column_info["constraints"].append(constraint_sql)
            if "AUTO_INCREMENT" in constraint_sql:
                column_info["is_serial"] = True
    return column_info


def parse_create_tables(sql_script: str, dialect: str = 'postgres') -> Dict[str, Any]:
    """
    Parse SQL CREATE TABLE statements and extract schema details.

    Parameters
    ----------
    sql_script : str
        The SQL script containing CREATE TABLE statements.
    dialect : str, optional
        The SQL dialect to parse. Defaults to 'postgres'.

    Returns
    -------
    dict
        A dictionary where keys are table names and values contain metadata,
        including columns, foreign keys, primary keys, unique constraints, and check constraints.
    """
    logger.info("Starting to parse SQL script with dialect '%s'", dialect)
    parsed = sqlglot.parse(sql_script, read=dialect)
    logger.info("Parsed %d statements from SQL script.", len(parsed))
    tables: Dict[str, Any] = {}

    for statement in parsed:
        if isinstance(statement, Create):
            schema = statement.this
            # Ensure a valid schema/table reference exists.
            if not hasattr(schema, "this"):
                continue

            table_expr = schema.this
            if not isinstance(table_expr, Table):
                continue
            table_name = table_expr.name
            columns: List[Dict[str, Any]] = []
            table_foreign_keys: List[Dict[str, Any]] = []
            table_unique_constraints: List[List[str]] = []
            table_primary_key: List[str] = []
            table_checks: List[str] = []
            logger.info("Parsing table '%s'", table_name)

            for expression in schema.expressions:
                # COLUMN DEFINITIONS
                if isinstance(expression, ColumnDef):
                    col_info = _parse_column_def(expression)
                    if not col_info["name"]:
                        col_info["name"] = f"col_{len(columns) + 1}"
                    columns.append(col_info)
                    if "PRIMARY KEY" in col_info["constraints"]:
                        table_primary_key.append(col_info["name"])
                        table_unique_constraints.append([col_info["name"]])
                    if "UNIQUE" in col_info["constraints"]:
                        table_unique_constraints.append([col_info["name"]])
                    if col_info["foreign_key"]:
                        table_foreign_keys.append(col_info["foreign_key"])
                    # Also capture column-level CHECKs to the table_checks list.
                    for c in col_info["constraints"]:
                        if c.upper().startswith("CHECK"):
                            if c not in table_checks:
                                table_checks.append(c.replace("CHECK ", "").strip())
                # TABLE-LEVEL FOREIGN KEY
                elif isinstance(expression, ForeignKey):
                    fk_columns = [col.name for col in expression.expressions]
                    ref_table, ref_columns = _extract_reference(expression.args.get("reference"))
                    fk_info = {
                        "columns": fk_columns,
                        "ref_table": ref_table,
                        "ref_columns": ref_columns
                    }
                    on_delete = expression.args.get("on_delete")
                    if on_delete:
                        fk_info["on_delete"] = on_delete.sql() if hasattr(on_delete, "sql") else str(on_delete)
                    table_foreign_keys.append(fk_info)
                # TABLE-LEVEL PRIMARY KEY
                elif isinstance(expression, PrimaryKey):
                    pk_columns = []
                    for idx, col in enumerate(expression.expressions):
                        col_name = getattr(col, "name", "").strip()
                        if not col_name:
                            col_name = f"col_{idx + 1}"
                            logger.warning(
                                "Found primary key column with empty name in table '%s'. Using default '%s'.",
                                table_expr.name, col_name)
                        pk_columns.append(col_name)
                    table_primary_key.extend(pk_columns)
                    table_unique_constraints.append(pk_columns)
                # TABLE-LEVEL CONSTRAINTS (UNIQUE, PK, FK, CHECK, etc.)
                elif isinstance(expression, Constraint):
                    if not expression.expressions:
                        continue
                    first_expr = expression.expressions[0]
                    if isinstance(first_expr, UniqueColumnConstraint):
                        unique_columns = [col.name for col in first_expr.this.expressions]
                        table_unique_constraints.append(unique_columns)
                    elif isinstance(first_expr, PrimaryKey):
                        pk_columns = []
                        for idx, col in enumerate(first_expr.expressions):
                            col_name = getattr(col, "name", "").strip()
                            if not col_name:
                                col_name = f"col_{idx + 1}"
                                logger.warning(
                                    "Found primary key column with empty name in table '%s'. Using default '%s'.",
                                    table_expr.name, col_name)
                            pk_columns.append(col_name)
                        table_primary_key.extend(pk_columns)
                        table_unique_constraints.append(pk_columns)
                    elif isinstance(first_expr, ForeignKey):
                        fk_columns = [col.name for col in first_expr.expressions]
                        ref_table, ref_columns = _extract_reference(first_expr.args.get("reference"))
                        fk_info = {
                            "columns": fk_columns,
                            "ref_table": ref_table,
                            "ref_columns": ref_columns
                        }
                        on_delete = first_expr.args.get("on_delete")
                        if on_delete:
                            fk_info["on_delete"] = on_delete.sql() if hasattr(on_delete, "sql") else str(on_delete)
                        table_foreign_keys.append(fk_info)
                    elif isinstance(first_expr, CheckColumnConstraint):
                        check_expression = first_expr.args.get("this")
                        if check_expression:
                            check_sql = check_expression.sql()
                            if check_sql not in table_checks:
                                table_checks.append(check_sql)
                    elif "CHECK" in expression.sql().upper():
                        # Fallback: extract any CHECK clause from the raw SQL.
                        match = re.search(r'CHECK\s*\((.+)\)', expression.sql(), re.IGNORECASE)
                        if match:
                            check_expr = match.group(1).strip()
                            if check_expr not in table_checks:
                                table_checks.append(check_expr)
                # TABLE-LEVEL CHECK (if expressed as a dedicated Check expression)
                elif isinstance(expression, Check):
                    check_expression = expression.args.get("this")
                    if check_expression:
                        check_sql = check_expression.sql()
                        if check_sql not in table_checks:
                            table_checks.append(check_sql)
                else:
                    # As a final catch-all, if the raw SQL of the expression starts with "CHECK",
                    # try to extract its contents.
                    raw = expression.sql().strip()
                    if raw.upper().startswith("CHECK"):
                        match = re.search(r'CHECK\s*\((.+)\)', raw, re.IGNORECASE)
                        if match:
                            check_expr = match.group(1).strip()
                            if check_expr not in table_checks:
                                table_checks.append(check_expr)

            tables[table_name] = {
                "columns": columns,
                "foreign_keys": table_foreign_keys,
                "primary_key": table_primary_key,
                "unique_constraints": table_unique_constraints,
                "check_constraints": table_checks
            }
    logger.info("Finished parsing SQL script, found %d tables.", len(tables))
    return tables