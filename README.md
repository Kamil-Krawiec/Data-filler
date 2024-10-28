# Data Generation Tool

## Overview

This project provides a data generation tool that creates synthetic data for database tables based on predefined schemas and constraints.

## Classes and Functions

### `DataGenerator`
A class to generate synthetic data for database tables based on provided schemas and constraints.

### `__init__`
Initialize the CheckConstraintEvaluator and set up the expression parser.

### `build_foreign_key_map`
Build a mapping of foreign key relationships for quick lookup.

Returns:
    dict: A mapping where each key is a parent table, and the value is a list of child table relationships.

### `resolve_table_order`
Resolve the order in which tables should be processed based on foreign key dependencies.

Returns:
    list: Ordered list of table names.

### `initialize_primary_keys`
Initialize primary key counters for each table.

### `generate_initial_data`
Generate initial data for all tables without enforcing constraints.

### `generate_composite_primary_keys`
Generate data for a table with a composite primary key.

Args:
    table (str): Table name.
    num_rows (int): Number of rows to generate.

### `generate_primary_keys`
Generate primary key values for a table row.

Args:
    table (str): Table name.
    row (dict): Row data.

### `enforce_constraints`
Enforce all constraints on the generated data.

### `assign_foreign_keys`
Assign foreign key values to a table row.

Args:
    table (str): Table name.
    row (dict): Row data.

### `fill_remaining_columns`
Fill in the remaining columns of a table row.

### `enforce_not_null_constraints`
Enforce NOT NULL constraints on a table row.

Args:
    table (str): Table name.
    row (dict): Row data.

### `generate_column_value`
Generate a value for a column based on predefined values, mappings, and constraints.

Args:
    table (str): Table name.
    column (dict): Column schema.
    row (dict): Current row data.
    constraints (list): List of constraints relevant to the column.

Returns:
    Any: Generated value.

### `generate_value_based_on_type`
Generate a value based on the SQL data type.

Args:
    col_type (str): Column data type.

Returns:
    Any: Generated value.

### `is_foreign_key_column`
Check if a column is a foreign key in the specified table.

Args:
    table_p (str): Table name.
    col_name (str): Column name.

Returns:
    bool: True if the column is a foreign key, False otherwise.

### `enforce_unique_constraints`
Enforce unique constraints on a table row.

### `enforce_check_constraints`
Enforce CHECK constraints on a table row.

Args:
    table (str): Table name.
    row (dict): Row data.

### `get_column_info`
Get the column schema information for a specific column.

Args:
    table (str): Table name.
    col_name (str): Column name.

Returns:
    dict: Column schema.

### `generate_data`
Generate the data by running all steps.

Returns:
    dict: Generated data with constraints enforced.

### `export_as_sql_insert_query`
Export the generated data as SQL INSERT queries.

Returns:
    str: A string containing SQL INSERT queries.

### `repair_data`
Iterate through the data and remove any rows that violate constraints,
including cascading deletions to maintain referential integrity.

### `repair_table_data`
Repair data for a specific table.

Args:
    table (str): Table name.

### `is_row_valid`
Check if a row is valid by checking all constraints.

Args:
    table (str): Table name.
    row (dict): Row data.

Returns:
    tuple: (is_valid, violated_constraint)
        is_valid (bool): True if the row is valid, False otherwise.
        violated_constraint (str): Description of the violated constraint, or None if valid.

### `remove_dependent_data`
Recursively remove dependent rows in child tables.

Args:
    table (str): Table name where the row is removed.
    row (dict): The row data that was removed.

### `print_statistics`
Print statistics about the generated data.

### `CheckConstraintEvaluator`
A class to evaluate SQL CHECK constraints on row data.

### `_create_expression_parser`
Create a parser for SQL expressions used in CHECK constraints.

Returns:
    pyparsing.ParserElement: The parser for expressions.

### `extract_columns_from_check`
Extract column names from a CHECK constraint expression.

Args:
    check (str): CHECK constraint expression.

Returns:
    list: List of column names.

### `evaluate`
Evaluate a CHECK constraint expression.

Args:
    check_expression (str): CHECK constraint expression.
    row (dict): Current row data.

Returns:
    bool: True if the constraint is satisfied, False otherwise.

### `convert_sql_expr_to_python`
Convert a parsed SQL expression into a Python expression.

Args:
    parsed_expr: The parsed SQL expression.
    row (dict): Current row data.

Returns:
    str: The Python expression.

### `handle_operator`
Handle the conversion of parsed expressions containing operators to Python expressions.

Args:
    parsed_expr: The parsed SQL expression containing operators.
    row (dict): Current row data.

Returns:
    str: The converted Python expression.

### `extract`
Simulate SQL EXTRACT function.

Args:
    field (str): Field to extract (e.g., 'YEAR').
    source (datetime.date or datetime.datetime): Date/time source.

Returns:
    int: Extracted value.

### `regexp_like`
Simulate SQL REGEXP_LIKE function.

Args:
    value (str): The string to test.
    pattern (str): The regex pattern.

Returns:
    bool: True if the value matches the pattern.

### `like`
Simulate SQL LIKE operator using regex.

Args:
    value (str): The string to match.
    pattern (str): The pattern, with SQL wildcards.

Returns:
    bool: True if the value matches the pattern.

### `not_like`
Simulate SQL NOT LIKE operator.

Args:
    value (str): The string to match.
    pattern (str): The pattern, with SQL wildcards.

Returns:
    bool: True if the value does not match the pattern.

### `identifier_action`
None

### `extract_numeric_ranges`
Extract numeric ranges from constraints related to a specific column.

Args:
    constraints (list): List of constraint expressions.
    col_name (str): Name of the column to extract ranges for.

Returns:
    list: A list of tuples representing operators and their corresponding numeric values.

### `generate_numeric_value`
Generate a numeric value based on specified ranges and column type.

Args:
    ranges (list): A list of tuples representing numeric ranges and their operators.
    col_type (str): The data type of the column.

Returns:
    int or float: A randomly generated numeric value within the specified range.

### `generate_value_matching_regex`
Generate a value that matches a specified regex pattern.

Args:
    pattern (str): The regex pattern to match.

Returns:
    str: A randomly generated string that matches the given regex pattern.

### `extract_regex_pattern`
Extract regex patterns from constraints related to a specific column.

Args:
    constraints (list): List of constraint expressions.
    col_name (str): Name of the column to extract regex patterns for.

Returns:
    list: A list of regex patterns found in the constraints.

### `extract_allowed_values`
Extract allowed values from constraints related to a specific column.

Args:
    constraints (list): List of constraint expressions.
    col_name (str): Name of the column to extract allowed values for.

Returns:
    list: A list of allowed values specified in the constraints.

### `parse_create_tables`
Parses SQL CREATE TABLE statements and extracts table schema details,
including columns, data types, constraints, and foreign keys.

Args:
    sql_script (str): The SQL script containing CREATE TABLE statements.

Returns:
    dict: A dictionary where each key is a table name and the value is
          another dictionary containing columns and foreign keys.

## License

This project is licensed under the MIT License.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.
