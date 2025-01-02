import itertools
from datetime import datetime, date, timedelta

from faker import Faker

from .check_constraint_evaluator import CheckConstraintEvaluator
from .helpers import *

ParserElement.enablePackrat()


class DataGenerator:
    """
    Intelligent Data Generator for Automated Synthetic Database Population.

    The `DataGenerator` class is a powerful tool designed to create realistic synthetic data for database tables based on provided schemas and constraints. It automates the entire data generation process, ensuring that all relational dependencies and data integrity rules are meticulously respected. This tool is especially useful for populating PostgreSQL databases during development, testing, or demonstration phases.

    Key Features:
    - **Dependency Management:** Automatically resolves and establishes foreign key relationships between tables.
    - **Constraint Enforcement:** Adheres to all defined constraints, including NOT NULL, UNIQUE, CHECK, and FOREIGN KEY constraints.
    - **Customizable Data Generation:** Supports predefined values and custom faker functions for tailored data generation.
    - **Error Handling:** Implements a repair system to handle and rectify data inconsistencies by removing incompatible rows.
    - **Extensibility:** Easily extendable to accommodate additional data types and constraints as needed.
    """

    def __init__(self, tables, num_rows=10, predefined_values=None, column_type_mappings=None,
                 num_rows_per_table=None):
        """
        Initialize the DataGenerator with table schemas and configuration settings.

        Args:
            tables (dict): Parsed table schemas containing table definitions and constraints.
            num_rows (int, optional): Default number of rows to generate per table. Defaults to 10.
            predefined_values (dict, optional): Predefined values for specific columns to ensure consistency. Defaults to None.
            column_type_mappings (dict, optional): Mappings of column names to specific data generation functions or types. Defaults to None.
            num_rows_per_table (dict, optional): Specific number of rows to generate for each table. Overrides `num_rows` if provided. Defaults to None.
        """
        self.tables = tables
        self.num_rows = num_rows
        self.num_rows_per_table = num_rows_per_table or {}
        self.generated_data = {}
        self.primary_keys = {}
        self.unique_values = {}
        self.fake = Faker()
        self.table_order = self.resolve_table_order()
        self.initialize_primary_keys()
        self.check_evaluator = CheckConstraintEvaluator(schema_columns=self.get_all_column_names())
        self.foreign_key_map = self.build_foreign_key_map()
        self.predefined_values = predefined_values or {}
        self.column_type_mappings = column_type_mappings or {}

    def build_foreign_key_map(self) -> dict:
        """
        Construct a mapping of foreign key relationships between parent and child tables.

        This mapping facilitates the automatic assignment of foreign key values during data generation, ensuring referential integrity across related tables.

        Returns:
            dict: A dictionary where each key is a parent table name, and the value is a list of dictionaries detailing child table relationships, including referenced columns.
        """
        fk_map = {}
        for table_name, details in self.tables.items():
            for fk in details.get('foreign_keys', []):
                parent_table = fk['ref_table']
                child_table = table_name
                parent_columns = tuple(fk['ref_columns'])
                child_columns = tuple(fk['columns'])

                if parent_table not in fk_map:
                    fk_map[parent_table] = []

                fk_map[parent_table].append({
                    'child_table': child_table,
                    'parent_columns': parent_columns,
                    'child_columns': child_columns,
                })
        return fk_map

    def get_all_column_names(self) -> list:
        """
        Retrieve a comprehensive list of all column names across all tables.

        This method aggregates column names from every table defined in the schema, assisting in various data generation and validation processes.

        Returns:
            list: A list containing the names of all columns present in the database schema.
        """
        columns = set()
        for table in self.tables.values():
            for column in table['columns']:
                columns.add(column['name'])
        return list(columns)

    def resolve_table_order(self) -> list:
        """
        Determine the order for processing tables based on foreign key dependencies.

        By resolving table dependencies, this method ensures that parent tables
        are inserted before their corresponding child tables, preventing foreign key
        violations during data insertion.

        Returns:
            list: An ordered list of table names, respecting foreign key dependencies.

        Raises:
            Exception: If a circular dependency is detected among tables (i.e.,
                       no valid topological ordering is possible).
        """

        # 1) Initialize a dictionary to track dependencies of each table
        dependencies = {table: set() for table in self.tables}

        # 2) Fill in the dependency sets based on the foreign keys
        for table_name, details in self.tables.items():
            for fk in details.get('foreign_keys', []):
                ref_table = fk.get('ref_table')
                # Only consider valid foreign key references
                if ref_table and ref_table in self.tables:
                    dependencies[table_name].add(ref_table)

        # This list will store the resulting topological order
        table_order = []

        # 3) Repeatedly look for tables that have no remaining dependencies
        while dependencies:
            # Find all tables that currently have no dependencies
            no_deps = [t for t, deps in dependencies.items() if not deps]

            if not no_deps:
                # We failed to find a table with zero dependencies -> true cycle
                raise Exception(
                    "Circular dependency detected among tables. "
                    f"Remaining tables with unsatisfied dependencies: {dependencies}"
                )

            # 4) Move all those 'no dependency' tables into the result list
            for t in no_deps:
                table_order.append(t)
                # Remove them from the 'dependencies' dict entirely
                del dependencies[t]

            # 5) Remove the newly resolved tables from the remaining tables' dependency sets
            for t, deps in dependencies.items():
                deps.difference_update(no_deps)

        return table_order
    def initialize_primary_keys(self):
        """
        Initialize primary key counters for each table to ensure unique identifier generation.

        This method sets up counters for primary key columns, starting from 1, to facilitate the creation of unique primary key values for each row in every table.
        """
        for table in self.tables:
            self.primary_keys[table] = {}
            pk_columns = self.tables[table].get('primary_key', [])
            for pk in pk_columns:
                self.primary_keys[table][pk] = 1  # Start counting from 1

    def generate_initial_data(self):
        for table in self.table_order:
            self.generated_data[table] = []
            num_rows = self.num_rows_per_table.get(table, self.num_rows)
            pk_columns = self.tables[table].get('primary_key', [])

            if len(pk_columns) == 1:
                for _ in range(num_rows):
                    row = {}
                    self.generate_primary_keys(table, row)
                    self.generated_data[table].append(row)
            elif len(pk_columns) > 1:
                # Composite primary key; generate combinations
                self.generate_composite_primary_keys(table, num_rows)
            else:
                # No primary key; generate rows without primary key assignment
                for _ in range(num_rows):
                    row = {}
                    self.generated_data[table].append(row)

    def generate_composite_primary_keys(self, table: str, num_rows: int):
        pk_columns = self.tables[table]['primary_key']

        # Generate possible values for each primary key column
        pk_values = {}
        for pk in pk_columns:
            # If the primary key column is a foreign key, get values from the referenced table
            if self.is_foreign_key_column(table, pk):
                fk = next((fk for fk in self.tables[table]['foreign_keys'] if pk in fk['columns']), None)
                if fk and fk['ref_table'] in self.generated_data:
                    ref_table = fk['ref_table']
                    ref_column = fk['ref_columns'][fk['columns'].index(pk)]
                    ref_data = self.generated_data[ref_table]
                    if ref_data:
                        pk_values[pk] = [row[ref_column] for row in ref_data]
                    else:
                        # If referenced table has no data, assign None or handle accordingly
                        pk_values[pk] = [None]
                else:
                    # If foreign key references a non-existent table, assign None or handle accordingly
                    pk_values[pk] = [None]
            else:
                # Generate a range of values for the primary key column
                pk_values[pk] = list(range(1, num_rows + 1))

        # Generate all possible combinations of primary key values
        combinations = list(itertools.product(*(pk_values[pk] for pk in pk_columns)))
        random.shuffle(combinations)

        # Adjust the number of rows if not enough unique combinations
        max_possible_rows = len(combinations)
        if max_possible_rows < num_rows:
            print(
                f"Not enough unique combinations for composite primary key in table '{table}'. Adjusting number of rows to {max_possible_rows}.")
            num_rows = max_possible_rows

        # Generate rows using the unique combinations
        for i in range(num_rows):
            row = {}
            for idx, pk in enumerate(pk_columns):
                row[pk] = combinations[i][idx]
            self.generated_data[table].append(row)

    def generate_primary_keys(self, table: str, row: dict):
        """
        Assign unique primary key values to a given row in a specified table.
        If the PK column is_serial=True, we treat it as auto-increment integer.
        Otherwise, we might generate something else (e.g., random strings).
        """
        pk_columns = self.tables[table].get('primary_key', [])
        for pk_col in pk_columns:
            col_info = self.get_column_info(table, pk_col)
            if not col_info:
                continue
            col_type = col_info['type'].upper()

            # is_serial or numeric => auto-increment
            if col_info.get("is_serial") or re.search(r'(INT|BIGINT|SMALLINT|DECIMAL|NUMERIC)', col_type):
                row[pk_col] = self.primary_keys[table][pk_col]
                self.primary_keys[table][pk_col] += 1
            else:
                # It's a non-numeric PK, so generate text or whatever is suitable
                length_match = re.search(r'\((\d+)\)', col_type)
                length = int(length_match.group(1)) if length_match else 5
                row[pk_col] = self.fake.lexify(text='?' * length)

    def enforce_constraints(self):
        """
        Enforce all defined constraints on the generated data across all tables.

        This method applies NOT NULL, UNIQUE, and CHECK constraints to ensure data integrity. It also manages the assignment of foreign keys based on established relationships.
        """
        for table in self.table_order:
            self.unique_values[table] = {}
            unique_constraints = self.tables[table].get('unique_constraints', []).copy()
            # Include primary keys in unique constraints
            primary_key = self.tables[table].get('primary_key', [])
            if primary_key:
                unique_constraints.append(primary_key)
            for unique_cols in unique_constraints:
                self.unique_values[table][tuple(unique_cols)] = set()

            for row in self.generated_data[table]:
                self.assign_foreign_keys(table, row)
                self.fill_remaining_columns(table, row)
                self.enforce_not_null_constraints(table, row)
                self.enforce_unique_constraints(table, row)
                self.enforce_check_constraints(table, row)

    def assign_foreign_keys(self, table: str, row: dict):
        """
        Automatically assign foreign key values to a table row based on
        established relationships, including support for composite keys
        and partially pre-filled columns.
        """
        fks = self.tables[table].get('foreign_keys', [])
        for fk in fks:
            fk_columns = fk['columns']  # e.g. ['row', 'seat', 'theater_id']
            ref_table = fk['ref_table']  # e.g. 'Seats'
            ref_columns = fk['ref_columns']  # e.g. ['row', 'seat', 'theater_id']

            # We'll check child's existing FK columns to see if they're set
            child_values = [row.get(fc) for fc in fk_columns]
            all_set = all(v is not None for v in child_values)
            partially_set = any(v is not None for v in child_values) and not all_set

            # Potential parent rows
            parent_data = self.generated_data[ref_table]

            # ─────────────────────────────────────────
            # 1) If all columns are already set, see if there's a matching parent row
            # ─────────────────────────────────────────
            if all_set:
                matching_parents = [
                    p for p in parent_data
                    if all(p[rc] == row[fc] for rc, fc in zip(ref_columns, fk_columns))
                ]
                if matching_parents:
                    # We do nothing: child's columns already match a valid parent
                    continue
                else:
                    # No match found → pick a valid random parent & overwrite child's columns
                    chosen_parent = random.choice(parent_data)
                    for rc, fc in zip(ref_columns, fk_columns):
                        row[fc] = chosen_parent[rc]
                continue

            # ─────────────────────────────────────────
            # 2) If *some* columns are set (partial), do a partial match
            # ─────────────────────────────────────────
            if partially_set:
                possible_parents = []
                for p in parent_data:
                    is_candidate = True
                    for rc, fc in zip(ref_columns, fk_columns):
                        child_val = row.get(fc)
                        # If child_val is set, parent must match
                        if child_val is not None and p[rc] != child_val:
                            is_candidate = False
                            break
                    if is_candidate:
                        possible_parents.append(p)

                if not possible_parents:
                    # No partial match => pick random parent
                    chosen_parent = random.choice(parent_data)
                else:
                    # Among partial matches, pick one at random
                    chosen_parent = random.choice(possible_parents)

                # Fill any missing columns from the chosen parent
                for rc, fc in zip(ref_columns, fk_columns):
                    if row.get(fc) is None:
                        row[fc] = chosen_parent[rc]
                continue

            # ─────────────────────────────────────────
            # 3) If none of the columns are set, pick a random parent row
            # ─────────────────────────────────────────
            chosen_parent = random.choice(parent_data)
            for rc, fc in zip(ref_columns, fk_columns):
                row[fc] = chosen_parent[rc]

    def fill_remaining_columns(self, table: str, row: dict):
        """
        Populate all remaining columns in a table row with appropriate synthetic data.

        This method handles the generation of data for columns that are not primary or foreign keys, utilizing predefined values and custom mappings to ensure realistic data generation.

        Args:
            table (str): The name of the table where the row resides.
            row (dict): The dictionary representing the row data to be populated.
        """
        columns = self.tables[table]['columns']
        for column in columns:
            col_name = column['name']
            if col_name in row:
                continue  # Skip columns that are already generated

            # Collect constraints relevant to this column
            col_constraints = []
            # Add column-specific constraints
            constraints = column.get('constraints', [])
            col_constraints.extend(constraints)

            # Add table-level check constraints
            check_constraints = self.tables[table].get('check_constraints', [])
            for constraint in check_constraints:
                if col_name in constraint:
                    col_constraints.append(constraint)

            # If is_serial but not a PK, handle auto-increment:
            if column.get('is_serial'):
                # If we haven't set up a separate counter for this col, do so now
                if col_name not in self.primary_keys[table]:
                    self.primary_keys[table][col_name] = 1
                row[col_name] = self.primary_keys[table][col_name]
                self.primary_keys[table][col_name] += 1
            else:
                row[col_name] = self.generate_column_value(table, column, row, constraints=col_constraints)

    def enforce_not_null_constraints(self, table: str, row: dict):
        """
        Ensure that all NOT NULL constraints are satisfied by populating missing values in a table row.

        Args:
            table (str): The name of the table where the row resides.
            row (dict): The dictionary representing the row data to be checked and populated.
        """
        for column in self.tables[table]['columns']:
            col_name = column['name']
            constraints = column.get('constraints', [])
            if 'NOT NULL' in constraints and row.get(col_name) is None:
                row[col_name] = self.generate_column_value(table, column, row, constraints=constraints)

    def generate_column_value(
            self,
            table: str,
            column: dict,
            row: dict,
            constraints: list = None
    ):
        """
        Generate a synthetic value for a specific column in a table row, considering predefined values and constraints.

        Args:
            table (str): The name of the table containing the column.
            column (dict): The schema information of the column for which to generate data.
            row (dict): The current state of the row being populated.
            constraints (list, optional): A list of constraints applicable to the column. Defaults to None.

        Returns:
            Any: A generated value that adheres to the column's data type and constraints.
        """
        constraints = constraints or []
        col_name = column['name']
        col_type = column['type'].upper()

        # Check for per-table predefined values
        predefined_values = None
        if table in self.predefined_values and col_name in self.predefined_values[table]:
            predefined_values = self.predefined_values[table][col_name]
        elif 'global' in self.predefined_values and col_name in self.predefined_values['global']:
            predefined_values = self.predefined_values['global'][col_name]

        if predefined_values is not None:
            if isinstance(predefined_values, list):
                return random.choice(predefined_values)
            else:
                return predefined_values

        # Check for per-table column type mappings
        mapping_entry = None
        if table in self.column_type_mappings and col_name in self.column_type_mappings[table]:
            mapping_entry = self.column_type_mappings[table][col_name]
        elif 'global' in self.column_type_mappings and col_name in self.column_type_mappings['global']:
            mapping_entry = self.column_type_mappings['global'][col_name]

        if mapping_entry:
            if isinstance(mapping_entry, dict):
                generator = mapping_entry.get('generator')
                if callable(generator):
                    return generator(self.fake, row)
                else:
                    return generator
            elif callable(mapping_entry):
                return mapping_entry(self.fake, row)
            else:
                # Use faker attribute or fixed value
                return getattr(self.fake, mapping_entry)() if hasattr(self.fake, mapping_entry) else mapping_entry

        # Check for regex constraints
        regex_patterns = extract_regex_pattern(constraints, col_name)
        if regex_patterns:
            # For simplicity, use the first pattern
            pattern = regex_patterns[0]
            return generate_value_matching_regex(pattern)

        # Check for allowed values (IN constraints)
        allowed_values = extract_allowed_values(constraints, col_name)
        if allowed_values:
            return random.choice(allowed_values)

        # Check for numeric ranges
        numeric_ranges = extract_numeric_ranges(constraints, col_name)
        if numeric_ranges:
            return generate_numeric_value(numeric_ranges, col_type)

        return self.generate_value_based_on_type(col_type)

    def generate_value_based_on_type(self, col_type: str):
        """
        Generate a synthetic value based on the SQL data type of a column.

        Args:
            col_type (str): The SQL data type of the column.

        Returns:
            Any: A synthetic value appropriate for the specified data type.
        """
        is_unsigned = False
        if col_type.upper().startswith('U'):
            is_unsigned = True
            col_type = col_type[1:]  # Remove the leading 'U' so the rest of the logic matches e.g. 'INT', 'BIGINT'

        col_type = col_type.upper()

        if re.match(r'.*\b(INT|INTEGER|SMALLINT|BIGINT)\b.*', col_type):
            min_val = 0 if is_unsigned else -10000
            return random.randint(min_val, 10000)
        elif re.match(r'.*\b(DECIMAL|NUMERIC)\b.*', col_type):
            # Similar logic for DECIMAL if needed
            precision, scale = 10, 2
            match = re.search(r'\((\d+),\s*(\d+)\)', col_type)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
            max_value = 10 ** (precision - scale) - 1

            # If it's unsigned, ensure the minimum is 0
            min_dec = 0.0 if is_unsigned else -9999.0  # or 0 if you prefer all positives
            return round(random.uniform(min_dec, max_value), scale)

        elif re.match(r'.*\b(FLOAT|REAL|DOUBLE PRECISION|DOUBLE)\b.*', col_type):
            return random.uniform(0, 10000)
        elif re.match(r'.*\b(BOOLEAN|BOOL)\b.*', col_type):
            return random.choice([True, False])
        elif re.match(r'.*\b(DATE)\b.*', col_type):
            return self.fake.date_object()
        elif re.match(r'.*\b(TIMESTAMP|DATETIME)\b.*', col_type):
            return self.fake.date_time()
        elif re.match(r'.*\b(TIME)\b.*', col_type):
            return self.fake.time()
        elif re.match(r'.*\b(CHAR|NCHAR|VARCHAR|NVARCHAR|CHARACTER VARYING|TEXT)\b.*', col_type):
            length_match = re.search(r'\((\d+)\)', col_type)
            length = int(length_match.group(1)) if length_match else 255
            if length >= 5:
                # Use fake.text for lengths >= 5
                return self.fake.text(max_nb_chars=length)[:length]
            elif length > 0:
                # Use fake.lexify for lengths < 5
                return self.fake.lexify(text='?' * length)
            else:
                # Length is zero or negative; return an empty string
                return ''
        else:
            # Default to a random word for unknown types
            return self.fake.word()

    def is_foreign_key_column(self, table_p: str, col_name: str) -> bool:
        """
        Determine whether a specific column in a table is a foreign key.

        Args:
            table_p (str): The name of the table containing the column.
            col_name (str): The name of the column to check.

        Returns:
            bool: True if the column is a foreign key, False otherwise.
        """
        fks = self.tables[table_p].get('foreign_keys', [])
        for fk in fks:
            if col_name in fk['columns']:
                return True
        return False

    def enforce_unique_constraints(self, table: str, row: dict):
        """
        Enforce UNIQUE constraints on a table row to ensure data uniqueness.

        Args:
            table (str): The name of the table where the row resides.
            row (dict): The dictionary representing the row data to be validated.
        """

        unique_constraints = self.tables[table].get('unique_constraints', []).copy()
        for unique_cols in unique_constraints:
            unique_key = tuple(row[col] for col in unique_cols)
            unique_set = self.unique_values[table][tuple(unique_cols)]
            while unique_key in unique_set:
                for col in unique_cols:
                    # Do not modify foreign key columns
                    if self.is_foreign_key_column(table, col):
                        continue
                    column = self.get_column_info(table, col)
                    row[col] = self.generate_column_value(table, column, row, constraints=unique_constraints)
                unique_key = tuple(row[col] for col in unique_cols)
            unique_set.add(unique_key)

    def enforce_check_constraints(self, table: str, row: dict):
        """
        Enforce CHECK constraints on a table row to validate data against custom conditions.

        Args:
            table (str): The name of the table where the row resides.
            row (dict): The dictionary representing the row data to be validated.
        """
        check_constraints = self.tables[table].get('check_constraints', [])
        for check in check_constraints:
            conditions = self.check_evaluator.extract_conditions(check)
            while not self.check_evaluator.evaluate(check, row):
                for col_name, conds in conditions.items():
                    column = self.get_column_info(table, col_name)
                    if column:
                        row[col_name] = self.generate_value_based_on_conditions(row, column, conds)

    def generate_value_based_on_conditions(
            self,
            row: dict,
            column: dict,
            conditions: list
    ):
        """
        Generate a column value that satisfies specific conditional constraints.

        Args:
            row (dict): The current state of the row being populated.
            column (dict): The schema information of the column for which to generate data.
            conditions (list): A list of conditions extracted from CHECK constraints.

        Returns:
            Any: A generated value that meets all specified conditions.
        """
        col_type = column['type'].upper()
        min_value = None
        max_value = None
        other_column_conditions = []

        for condition in conditions:
            operator = condition['operator']
            value = condition['value']
            if isinstance(value, (int, float, date)):
                if operator in ('>=', '>'):
                    min_candidate = value + (1 if operator == '>' else 0)
                    min_value = max(min_value, min_candidate) if min_value is not None else min_candidate
                elif operator in ('<=', '<'):
                    max_candidate = value - (1 if operator == '<' else 0)
                    max_value = min(max_value, max_candidate) if max_value is not None else max_candidate
            elif isinstance(value, str) and value in self.get_all_column_names():
                # It's another column
                other_column_conditions.append((operator, value))
            else:
                # Handle other types if necessary
                pass

        # Generate initial value based on min and max
        if 'INT' in col_type or 'DECIMAL' in col_type or 'NUMERIC' in col_type:
            min_value = min_value if min_value is not None else 1
            max_value = max_value if max_value is not None else 10000
            generated_value = random.randint(int(min_value), int(max_value))
        elif 'DATE' in col_type:
            min_date = min_value if isinstance(min_value, date) else date(1900, 1, 1)
            max_date = max_value if isinstance(max_value, date) else date.today()
            delta = (max_date - min_date).days
            random_days = random.randint(0, delta)
            generated_value = min_date + timedelta(days=random_days)
        else:
            # Default generation for other types
            generated_value = self.generate_value_based_on_type(col_type)

        # Adjust the generated value to satisfy conditions involving other columns
        for operator, other_col in other_column_conditions:
            other_value = row.get(other_col)
            if other_value is None:
                # Generate the other column value first
                other_column_info = self.get_column_info(column['table'], other_col)
                if other_column_info:
                    row[other_col] = self.generate_column_value(column['table'], other_column_info, row)
                    other_value = row[other_col]
                else:
                    continue  # Cannot proceed without the other column

            # Adjust generated_value based on the operator and other_value
            if 'INT' in col_type or 'DECIMAL' in col_type or 'NUMERIC' in col_type:
                if operator == '>':
                    generated_value = max(generated_value, other_value + 1)
                elif operator == '>=':
                    generated_value = max(generated_value, other_value)
                elif operator == '<':
                    generated_value = min(generated_value, other_value - 1)
                elif operator == '<=':
                    generated_value = min(generated_value, other_value)
            elif 'DATE' in col_type:
                if operator == '>':
                    generated_value = max(generated_value, other_value + timedelta(days=1))
                elif operator == '>=':
                    generated_value = max(generated_value, other_value)
                elif operator == '<':
                    generated_value = min(generated_value, other_value - timedelta(days=1))
                elif operator == '<=':
                    generated_value = min(generated_value, other_value)
            else:
                # Handle other types if necessary
                pass

        return generated_value

    def get_column_info(self, table: str, col_name: str) -> dict:
        """
        Retrieve schema information for a specific column in a table.

        Args:
            table (str): The name of the table containing the column.
            col_name (str): The name of the column to retrieve information for.

        Returns:
            dict: A dictionary containing the column's schema details.
        """
        for col in self.tables[table]['columns']:
            if col['name'] == col_name:
                return col
        return None

    def generate_data(self) -> dict:
        """
        Execute the complete data generation process, including initial data creation and constraint enforcement.

        This method orchestrates the entire workflow of data generation, ensuring that all tables are populated in the correct order and that all constraints are duly enforced.

        Returns:
            dict: A dictionary containing the generated data for each table, structured by table name.
        """
        self.generate_initial_data()
        self.enforce_constraints()
        self.repair_data()
        self.print_statistics()
        return self.generated_data

    def export_as_sql_insert_query(self) -> str:
        """
        Export the generated synthetic data as SQL INSERT queries.

        This method formats the generated data into executable SQL INSERT statements, allowing for easy population of the target PostgreSQL database.

        Returns:
            str: A string containing SQL INSERT queries for all populated tables.
        """
        insert_queries = []

        for table_name, records in self.generated_data.items():
            if not records:
                continue  # Skip if there's no data for the table

            # Get column names from the table schema
            columns = [col['name'] for col in self.tables[table_name]['columns']]

            # Start constructing the INSERT statement
            insert_prefix = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES"

            # Collect values for each record
            values_list = []
            for record in records:
                values = []
                for col in columns:
                    value = record.get(col)
                    if value is None:
                        values.append('NULL')
                    elif isinstance(value, str):
                        # Escape single quotes in strings
                        escaped_value = value.replace("'", "''")
                        values.append(f"'{escaped_value}'")
                    elif isinstance(value, datetime):
                        values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
                    elif isinstance(value, date):
                        values.append(f"'{value.strftime('%Y-%m-%d')}'")
                    elif isinstance(value, bool):
                        values.append('TRUE' if value else 'FALSE')
                    else:
                        values.append(str(value))
                values_str = f"({', '.join(values)})"
                values_list.append(values_str)

            # Combine the INSERT prefix and values
            insert_query = f"{insert_prefix}\n" + ",\n".join(values_list) + ";"
            insert_queries.append(insert_query)

        # Combine all INSERT queries into a single string
        return "\n\n".join(insert_queries)

    def repair_data(self):
        """
        Identify and remove any rows that violate defined constraints to maintain data integrity.

        This repair system scans the generated data for constraint violations and removes offending rows. It also handles cascading deletions in child tables to preserve referential integrity.
        """
        for table in self.table_order:
            self.repair_table_data(table)

    def repair_table_data(self, table: str):
        """
        Cleanse data in a specific table by removing rows that violate constraints.

        Args:
            table (str): The name of the table to repair.
        """
        valid_rows = []
        deleted_rows = 0
        for row in self.generated_data[table]:
            is_valid, violated_constraint = self.is_row_valid(table, row)
            if is_valid:
                valid_rows.append(row)
            else:
                deleted_rows += 1
                print(f"[Repair] Row deleted from table '{table}' due to constraint violation:")
                print(f"    Row data: {row}")
                print(f"    Violated constraint: {violated_constraint}")
                # Remove dependent data in child tables
                self.remove_dependent_data(table, row)
        self.generated_data[table] = valid_rows
        if deleted_rows > 0:
            print(f"[Repair] Deleted {deleted_rows} row(s) from table '{table}' during repair.")

    def is_row_valid(self, table: str, row: dict) -> tuple:
        """
        Validate a single row against all applicable constraints.

        Args:
            table (str): The name of the table where the row resides.
            row (dict): The dictionary representing the row data to be validated.

        Returns:
            tuple: A tuple containing a boolean indicating validity and a string describing the violated constraint, if any.
        """
        # Check NOT NULL constraints
        for column in self.tables[table]['columns']:
            col_name = column['name']
            constraints = column.get('constraints', [])
            if 'NOT NULL' in constraints and row.get(col_name) is None:
                return False, f"NOT NULL constraint on column '{col_name}'"

        # Check UNIQUE constraints
        unique_constraints = self.tables[table].get('unique_constraints', [])
        for unique_cols in unique_constraints:
            unique_key = tuple(row.get(col) for col in unique_cols)
            if None in unique_key:
                return False, f"UNIQUE constraint on columns {unique_cols} with NULL values"
            # Note: Since uniqueness is enforced during data generation, we assume it's valid here

        # Check CHECK constraints
        check_constraints = self.tables[table].get('check_constraints', [])
        for check in check_constraints:
            if not self.check_evaluator.evaluate(check, row):
                return False, f"CHECK constraint '{check}' failed"

        # All constraints passed
        return True, None

    def remove_dependent_data(self, table: str, row: dict):
        """
        Recursively remove dependent rows in child tables that reference a deleted parent row.

        Args:
            table (str): The name of the parent table from which a row was deleted.
            row (dict): The dictionary representing the deleted row data.
        """
        if table not in self.foreign_key_map:
            return

        for fk in self.foreign_key_map[table]:
            child_table = fk['child_table']
            parent_columns = fk['parent_columns']
            child_columns = fk['child_columns']

            # Build a tuple of values to match in child table
            parent_values = tuple(row.get(col) for col in parent_columns)

            # Filter out rows in child table that reference the removed parent row
            valid_child_rows = []
            deleted_rows = 0
            for child_row in self.generated_data.get(child_table, []):
                child_values = tuple(child_row.get(col) for col in child_columns)
                if child_values != parent_values:
                    valid_child_rows.append(child_row)
                else:
                    deleted_rows += 1
                    print(
                        f"[Repair] Row deleted from table '{child_table}' due to parent row deletion in '{table}': {child_row}")
                    # Recursively remove dependent data in lower-level child tables
                    self.remove_dependent_data(child_table, child_row)

            if deleted_rows > 0:
                print(
                    f"[Repair] Deleted {deleted_rows} dependent row(s) from table '{child_table}' due to deletions in '{table}'.")
            self.generated_data[child_table] = valid_child_rows

    def print_statistics(self):
        """
        Display statistics about the generated data, including the number of rows per table.

        This method provides a summary of the data generation process, helping users understand the scope and distribution of the synthetic data created.
        """
        print("\nData Generation Statistics:")
        for table in self.table_order:
            row_count = len(self.generated_data.get(table, []))
            print(f"Table '{table}': {row_count} row(s) generated.")
