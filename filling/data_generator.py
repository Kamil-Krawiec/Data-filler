import itertools
import logging
import os
import re
import json
import csv
import random
import numpy as np
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from faker import Faker

from .check_constraint_evaluator import CheckConstraintEvaluator
from .helpers import (
    extract_regex_pattern, generate_value_matching_regex,
    extract_allowed_values, extract_numeric_ranges,
    generate_numeric_value
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


class DataGenerator:
    """
    Intelligent Data Generator for Automated Synthetic Database Population.
    """

    def __init__(self, tables, num_rows=10, predefined_values=None, column_type_mappings=None,
                 num_rows_per_table=None):
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
        self.column_info_cache = {}
        self.foreign_key_cache = {}

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

    def _generate_table_initial_data(self, table: str):
        """
        Generate initial data for a single table.
        This helper is used in parallel generation.
        """
        self.generated_data[table] = []
        num_rows = self.num_rows_per_table.get(table, self.num_rows)
        pk_columns = self.tables[table].get('primary_key', [])

        if len(pk_columns) == 1:
            # Generate single-column primary keys
            self.generate_primary_keys(table, num_rows)
        elif len(pk_columns) > 1:
            # Generate composite primary keys
            self.generate_composite_primary_keys(table, num_rows)
        else:
            # No primary key => generate empty rows
            for _ in range(num_rows):
                self.generated_data[table].append({})

    def generate_initial_data(self):
        """
        Generate initial data for all tables in parallel groups while preserving order.
        This method first computes a level (or depth) for each table so that tables
        with no dependencies (or whose parents are already generated) are processed concurrently.
        """
        # Compute levels for each table.
        levels = {}
        for table in self.table_order:
            # Get all foreign keys for the table
            foreign_keys = self.tables[table].get('foreign_keys', [])
            if not foreign_keys:
                levels[table] = 0
            else:
                # Level is one more than the maximum level of its referenced tables.
                max_level = 0
                for fk in foreign_keys:
                    ref_table = fk.get('ref_table')
                    if ref_table in levels:
                        max_level = max(max_level, levels[ref_table] + 1)
                    else:
                        # If reference not processed yet, assume level 0 (will be corrected later)
                        max_level = max(max_level, 0)
                levels[table] = max_level

        # Group tables by level.
        level_groups = {}
        for table, level in levels.items():
            level_groups.setdefault(level, []).append(table)

        # Process each level sequentially; within a level, process tables concurrently.
        for level in sorted(level_groups.keys()):
            tables_at_level = level_groups[level]
            with ThreadPoolExecutor(max_workers=len(tables_at_level)) as executor:
                # Submit generation tasks for each table in this level.
                futures = {executor.submit(self._generate_table_initial_data, table): table
                           for table in tables_at_level}
                # Wait for all tables in this level to complete.
                for future in as_completed(futures):
                    table = futures[future]
                    try:
                        future.result()
                        logger.info(f"Initial data generated for table '{table}' at level {level}.")
                    except Exception as e:
                        logger.error(f"Error generating data for table '{table}': {e}")

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
                        # If referenced table has no data, assign None
                        pk_values[pk] = [None]
                else:
                    # If FK references a non-existent table, assign None
                    pk_values[pk] = [None]
            else:
                col_info = self.get_column_info(table, pk)
                constraints = col_info.get('constraints', [])

                # We'll produce num_rows possible values by calling generate_column_value each time.
                generated_list = []
                for _ in range(num_rows):
                    # We pass a temporary empty row (or partial row) to generate_column_value
                    val = self.generate_column_value(table, col_info, {}, constraints)
                    generated_list.append(val)

                pk_values[pk] = generated_list

        # Now produce the Cartesian product of all PK columns
        combinations = list(set(itertools.product(*(pk_values[pk] for pk in pk_columns))))
        random.shuffle(combinations)

        # Adjust if not enough unique combinations
        max_possible_rows = len(combinations)
        if max_possible_rows < num_rows:
            logger.info(
                f"Not enough unique combinations for composite primary key in table '{table}'. "
                f"Adjusting number of rows to {max_possible_rows}."
            )
            num_rows = max_possible_rows

        # Create rows using the chosen number of combinations
        for i in range(num_rows):
            row = {}
            for idx, pk in enumerate(pk_columns):
                row[pk] = combinations[i][idx]
            self.generated_data[table].append(row)

    def generate_primary_keys(self, table: str, num_rows: int):
        pk_columns = self.tables[table].get('primary_key', [])
        if len(pk_columns) != 1:
            return  # Handle composite PK elsewhere.

        pk_col = pk_columns[0]
        col_info = self.get_column_info(table, pk_col)
        if not col_info:
            return

        col_type = col_info['type'].upper()

        if col_info.get("is_serial") or re.search(r'(INT|BIGINT|SMALLINT|DECIMAL|NUMERIC)', col_type):
            start_val = self.primary_keys[table][pk_col]
            # Use NumPy to generate a range of auto-increment values.
            values = np.arange(start_val, start_val + num_rows)
            new_rows = [{pk_col: int(value)} for value in values]
            self.primary_keys[table][pk_col] = start_val + num_rows
        else:
            # For non-numeric PKs, fallback to the current approach.
            constraints = col_info.get('constraints', [])
            used_values = set()
            values_list = []
            while len(values_list) < num_rows:
                tmp_val = self.generate_column_value(table, col_info, {}, constraints)
                if tmp_val not in used_values:
                    used_values.add(tmp_val)
                    values_list.append(tmp_val)
            new_rows = [{pk_col: val} for val in values_list]

        self.generated_data[table] = new_rows

    def process_row(self, table: str, row: dict) -> dict:
        """
        Process a single row for a given table by assigning foreign keys,
        filling remaining columns, and enforcing NOT NULL and CHECK constraints.
        (Unique constraints are enforced later sequentially.)
        """
        self.assign_foreign_keys(table, row)
        self.fill_remaining_columns(table, row)
        self.enforce_not_null_constraints(table, row)
        self.enforce_check_constraints(table, row)
        return row

    def enforce_constraints(self):
        """
        Enforce all defined constraints on the generated data across all tables.
        This method applies NOT NULL and CHECK constraints in parallel per row,
        then enforces UNIQUE constraints sequentially to avoid race conditions.
        """
        for table in self.table_order:
            # Set up unique constraints for the table
            self.unique_values[table] = {}
            unique_constraints = self.tables[table].get('unique_constraints', []).copy()
            primary_key = self.tables[table].get('primary_key', [])
            if primary_key:
                unique_constraints.append(primary_key)
            for unique_cols in unique_constraints:
                self.unique_values[table][tuple(unique_cols)] = set()

            rows = self.generated_data[table]
            processed_rows = [None] * len(rows)

            # Process each row in parallel while preserving order.
            with ThreadPoolExecutor() as executor:
                futures = {executor.submit(self.process_row, table, row): i for i, row in enumerate(rows)}
                for future in as_completed(futures):
                    idx = futures[future]
                    processed_rows[idx] = future.result()

            # Enforce UNIQUE constraints sequentially (to avoid concurrency issues)
            for row in processed_rows:
                self.enforce_unique_constraints(table, row)

            self.generated_data[table] = processed_rows

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
        is_unsigned = False
        if col_type.upper().startswith('U'):
            is_unsigned = True
            col_type = col_type[1:]
        col_type = col_type.upper()

        if re.match(r'.*\b(INT|INTEGER|SMALLINT|BIGINT)\b.*', col_type):
            min_val = 0 if is_unsigned else -10000
            # Using numpy randint to generate a single value
            return int(np.random.randint(min_val, 10001))
        elif re.match(r'.*\b(DECIMAL|NUMERIC)\b.*', col_type):
            precision, scale = 10, 2
            match = re.search(r'\((\d+),\s*(\d+)\)', col_type)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
            max_value = 10 ** (precision - scale) - 1
            min_dec = 0.0 if is_unsigned else -9999.0
            # Using numpy uniform to generate a float value
            return round(float(np.random.uniform(min_dec, max_value)), scale)
        elif re.match(r'.*\b(FLOAT|REAL|DOUBLE PRECISION|DOUBLE)\b.*', col_type):
            return float(np.random.uniform(0, 10000))
        # For non-numeric types, fallback to the existing logic.
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
                return self.fake.text(max_nb_chars=length)[:length]
            elif length > 0:
                return self.fake.lexify(text='?' * length)
            else:
                return ''
        else:
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
        Enforce CHECK constraints on a table row by repeatedly generating candidate
        values until all CHECK constraints evaluate to True.

        This implementation evaluates all constraints at once, collects candidate
        values for any violations, updates the row in one pass, and re-checks until
        the row is valid.
        """
        check_constraints = self.tables[table].get('check_constraints', [])

        # Loop until every constraint passes.
        while True:
            updates = {}  # Dictionary to collect candidate updates for all failing columns.
            for check in check_constraints:
                is_valid, candidate = self.check_evaluator.evaluate(check, row)
                if not is_valid:
                    # Extract conditions for this check.
                    conditions = self.check_evaluator.extract_conditions(check)
                    # For each column involved in the failing check, generate a candidate.
                    for col_name, conds in conditions.items():
                        column = self.get_column_info(table, col_name)
                        if column:
                            # Here, we assume that 'candidate' is the value your evaluator
                            # proposes for the column. If needed, you could call an independent
                            # candidate generator (e.g., self.generate_value_based_on_conditions(row, column, conds))
                            updates[col_name] = candidate
            if not updates:
                break  # All constraints satisfied.
            # Update all columns at once.
            for col, new_val in updates.items():
                row[col] = new_val

    def generate_value_based_on_conditions(self, row: dict, column: dict, conditions: list):
        """
        Generate a candidate value for a column that (hopefully) satisfies a set of CHECK conditions.
        Supports numeric ranges (including BETWEEN‑like constraints), date ranges, LIKE patterns for strings,
        booleans, and can incorporate conditions that refer to other columns.

        Args:
            row (dict): The current row state.
            column (dict): The schema information for the column.
            conditions (list): A list of condition dictionaries with keys 'operator' and 'value'.

        Returns:
            A candidate value of the appropriate type.
        """
        col_type = column['type'].upper()

        # If an equality condition is present, return that value.
        for cond in conditions:
            if cond['operator'] in ('=', '=='):
                return cond['value']

        # ----- Numeric Types -----
        if re.search(r'\b(INT|INTEGER|SMALLINT|BIGINT|DECIMAL|NUMERIC|FLOAT|REAL)\b', col_type):
            # Set default range based on type.
            if any(x in col_type for x in ['INT', 'INTEGER', 'SMALLINT', 'BIGINT']):
                lower_bound, upper_bound, epsilon = 1, 10000, 1
            else:
                lower_bound, upper_bound, epsilon = 1.0, 10000.0, 0.001
            # Process each condition.
            for cond in conditions:
                op = cond['operator']
                val = cond['value']
                # If the condition refers to another column, use the current row value.
                if isinstance(val, str) and val in self.get_all_column_names():
                    if val in row:
                        val = row[val]
                    else:
                        continue
                if op == '>':
                    lower_bound = max(lower_bound, val + epsilon)
                elif op == '>=':
                    lower_bound = max(lower_bound, val)
                elif op == '<':
                    upper_bound = min(upper_bound, val - epsilon)
                elif op == '<=':
                    upper_bound = min(upper_bound, val)
            # If bounds are inconsistent, use the lower_bound.
            if lower_bound > upper_bound:
                candidate = lower_bound
            else:
                candidate = (random.randint(int(lower_bound), int(upper_bound))
                             if any(x in col_type for x in ['INT', 'INTEGER', 'SMALLINT', 'BIGINT'])
                             else random.uniform(lower_bound, upper_bound))
            return candidate

        # ----- Date Types -----
        elif re.search(r'\b(DATE)\b', col_type):
            default_lower, default_upper = date(1900, 1, 1), date.today()
            lower_bound, upper_bound = default_lower, default_upper
            for cond in conditions:
                op = cond['operator']
                val = cond['value']
                if isinstance(val, str) and val in self.get_all_column_names():
                    if val in row:
                        val = row[val]
                    else:
                        continue
                if not isinstance(val, date):
                    try:
                        val = datetime.strptime(val, '%Y-%m-%d').date()
                    except Exception:
                        continue
                if op == '>':
                    lower_bound = max(lower_bound, val + timedelta(days=1))
                elif op == '>=':
                    lower_bound = max(lower_bound, val)
                elif op == '<':
                    upper_bound = min(upper_bound, val - timedelta(days=1))
                elif op == '<=':
                    upper_bound = min(upper_bound, val)
            if lower_bound > upper_bound:
                candidate = lower_bound
            else:
                delta = (upper_bound - lower_bound).days
                candidate = lower_bound + timedelta(days=random.randint(0, delta))
            return candidate

        # ----- String Types -----
        elif re.search(r'\b(CHAR|NCHAR|VARCHAR|NVARCHAR|TEXT)\b', col_type):
            # Look for a LIKE condition first.
            for cond in conditions:
                op = cond['operator'].upper()
                val = cond['value']
                if op == 'LIKE':
                    pattern = val.strip("'")
                    # Simple heuristics for common patterns:
                    if pattern.endswith('%'):
                        fixed = pattern[:-1]
                        candidate = fixed + ''.join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5))
                        return candidate
                    elif pattern.startswith('%'):
                        fixed = pattern[1:]
                        candidate = ''.join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5)) + fixed
                        return candidate
                    else:
                        # If no wildcard or ambiguous, return the fixed pattern.
                        return pattern
            # Fallback: generate a random string of a default or specified length.
            length = 20
            match = re.search(r'\((\d+)\)', col_type)
            if match:
                length = int(match.group(1))
            candidate = self.fake.lexify(text='?' * length)[:length]
            return candidate

        # ----- Boolean Types -----
        elif 'BOOL' in col_type:
            return random.choice([True, False])

        # ----- Fallback -----
        else:
            return self.generate_value_based_on_type(col_type)

    def get_column_info(self, table: str, col_name: str) -> dict:
        """
        Retrieve schema information for a specific column in a table.

        Args:
            table (str): The name of the table containing the column.
            col_name (str): The name of the column to retrieve information for.

        Returns:
            dict: A dictionary containing the column's schema details.
        """
        key = (table, col_name)
        if key not in self.column_info_cache:
            column_info = next((col for col in self.tables[table]['columns'] if col['name'] == col_name), None)
            self.column_info_cache[key] = column_info
        return self.column_info_cache[key]

    def generate_data(self, run_repair=True, print_stats=True) -> dict:
        logger.info("Starting data generation process.")
        logger.info("Generating initial data...")
        self.generate_initial_data()
        logger.info("Initial data generation completed.")
        logger.info("Enforcing constraints...")
        self.enforce_constraints()
        logger.info("Constraints enforced.")
        if run_repair:
            logger.info("Repairing data to remove violations...")
            self.repair_data()
            logger.info("Data repair process completed.")
        if print_stats:
            logger.info("Printing data generation statistics...")
            self.print_statistics()
        logger.info("Data generation process finished.")
        return self.generated_data

    def export_as_sql_insert_query(self, max_rows_per_insert: int = 1000) -> str:
        """
        Export the generated synthetic data as SQL INSERT queries, splitting rows into chunks
        of `max_rows_per_insert` to avoid exceeding database limits on a single INSERT.

        Args:
            max_rows_per_insert (int, optional): Max number of rows per INSERT statement. Defaults to 1000.

        Returns:
            str: A string containing SQL INSERT queries for all populated tables.
        """
        insert_queries = []

        for table_name, records in self.generated_data.items():
            if not records:
                continue  # Skip if there's no data for the table

            # Get column names from the table schema
            columns = [col['name'] for col in self.tables[table_name]['columns']]
            insert_prefix = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES"

            # We'll chunk the records into slices of size max_rows_per_insert
            for i in range(0, len(records), max_rows_per_insert):
                chunk = records[i: i + max_rows_per_insert]

                values_list = []
                for record in chunk:
                    row_values = []
                    for col in columns:
                        value = record.get(col)
                        if value is None:
                            row_values.append('NULL')
                        elif isinstance(value, str):
                            # Escape single quotes in strings
                            escaped_value = value.replace("'", "''")
                            row_values.append(f"'{escaped_value}'")
                        elif isinstance(value, datetime):
                            row_values.append(f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'")
                        elif isinstance(value, date):
                            row_values.append(f"'{value.strftime('%Y-%m-%d')}'")
                        elif isinstance(value, bool):
                            row_values.append('TRUE' if value else 'FALSE')
                        else:
                            row_values.append(str(value))

                    values_str = f"({', '.join(row_values)})"
                    values_list.append(values_str)

                # Combine the prefix and the chunk of values
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
                logger.info(f"[Repair] Row deleted from table '{table}' due to constraint violation:")
                logger.info(f"    Row data: {row}")
                logger.info(f"    Violated constraint: {violated_constraint}")
                # Remove dependent data in child tables
                self.remove_dependent_data(table, row)
        self.generated_data[table] = valid_rows
        if deleted_rows > 0:
            logger.info(f"[Repair] Deleted {deleted_rows} row(s) from table '{table}' during repair.")

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
                    logger.info(
                        f"[Repair] Row deleted from table '{child_table}' due to parent row deletion in '{table}': {child_row}")
                    # Recursively remove dependent data in lower-level child tables
                    self.remove_dependent_data(child_table, child_row)

            if deleted_rows > 0:
                logger.info(
                    f"[Repair] Deleted {deleted_rows} dependent row(s) from table '{child_table}' due to deletions in '{table}'.")
            self.generated_data[child_table] = valid_child_rows

    def print_statistics(self):
        """
        Display statistics about the generated data, including the number of rows per table.

        This method provides a summary of the data generation process, helping users understand the scope and distribution of the synthetic data created.
        """
        logger.info("Data Generation Statistics:")
        for table in self.table_order:
            row_count = len(self.generated_data.get(table, []))
            logger.info(f"Table '{table}': {row_count} row(s) generated.")

    def export_data_files(self, output_dir: str, file_type='SQL') -> None:
        """
        Export generated data as CSV and JSON for each table concurrently,
        plus a single .sql file containing all insert statements (processed sequentially).
        """
        file_type = file_type.upper()
        os.makedirs(output_dir, exist_ok=True)

        # For SQL export, we still export to a single file sequentially.
        if file_type == 'SQL':
            sql_path = os.path.join(output_dir, "data_inserts.sql")
            with open(sql_path, mode="w", encoding="utf-8") as f:
                insert_statements = self.export_as_sql_insert_query()
                f.write(insert_statements)

        # For CSV and JSON, export each table concurrently.
        def export_table(table_name: str):
            columns = [col['name'] for col in self.tables[table_name]['columns']]
            records = self.generated_data.get(table_name, [])

            if file_type == 'CSV':
                csv_path = os.path.join(output_dir, f"{table_name}.csv")
                with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=columns)
                    writer.writeheader()
                    for row in records:
                        writer.writerow({col: row.get(col, "") for col in columns})
                logger.info(f"Exported CSV for table '{table_name}'.")

            if file_type == 'JSON':
                json_path = os.path.join(output_dir, f"{table_name}.json")
                with open(json_path, mode="w", encoding="utf-8") as f:
                    json.dump(records, f, indent=2, default=str)
                logger.info(f"Exported JSON for table '{table_name}'.")

        # Use ThreadPoolExecutor to export tables in parallel.
        if file_type in ('CSV', 'JSON'):
            with ThreadPoolExecutor(max_workers=len(self.generated_data)) as executor:
                futures = {executor.submit(export_table, table): table for table in self.generated_data}
                for future in as_completed(futures):
                    table = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Error exporting data for table '{table}': {e}")
