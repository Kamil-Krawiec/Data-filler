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
    Simplified Data Generator for Automated Synthetic Database Population.
    """

    def __init__(self, tables, num_rows=10, predefined_values=None,
                 column_type_mappings=None, num_rows_per_table=None):
        self.tables = tables
        self.num_rows = num_rows
        self.num_rows_per_table = num_rows_per_table or {}
        self.generated_data = {}
        self.primary_keys = {}
        self.unique_values = {}
        self.fake = Faker()
        self.table_order = self.resolve_table_order()
        self.initialize_primary_keys()
        self.check_evaluator = CheckConstraintEvaluator(
            schema_columns=self.get_all_column_names()
        )
        self.foreign_key_map = self.build_foreign_key_map()
        self.predefined_values = predefined_values or {}
        self.column_type_mappings = column_type_mappings or {}
        self.column_info_cache = {}
        # Removed foreign_key_cache and other extraneous caches

    def build_foreign_key_map(self) -> dict:
        """
        Construct a mapping of foreign key relationships between parent and child tables.
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
        """
        columns = set()
        for table in self.tables.values():
            for column in table['columns']:
                columns.add(column['name'])
        return list(columns)

    def resolve_table_order(self) -> list:
        """
        Determine the order for processing tables based on foreign key dependencies.
        """
        dependencies = {table: set() for table in self.tables}

        for table_name, details in self.tables.items():
            for fk in details.get('foreign_keys', []):
                ref_table = fk.get('ref_table')
                if ref_table and ref_table in self.tables:
                    dependencies[table_name].add(ref_table)

        table_order = []
        while dependencies:
            no_deps = [t for t, deps in dependencies.items() if not deps]
            if not no_deps:
                raise Exception("Circular dependency detected among tables.")
            for t in no_deps:
                table_order.append(t)
                del dependencies[t]
            for t, deps in dependencies.items():
                deps.difference_update(no_deps)

        return table_order

    def initialize_primary_keys(self):
        """
        Initialize primary key counters for each table.
        """
        for table in self.tables:
            self.primary_keys[table] = {}
            pk_columns = self.tables[table].get('primary_key', [])
            for pk in pk_columns:
                self.primary_keys[table][pk] = 1

    def generate_initial_data(self):
        """
        Generate initial (empty or PK-only) rows for each table in order.
        """
        for table in self.table_order:
            self._generate_table_initial_data(table)

    def _generate_table_initial_data(self, table: str):
        self.generated_data[table] = []
        num_rows = self.num_rows_per_table.get(table, self.num_rows)
        pk_columns = self.tables[table].get('primary_key', [])

        if len(pk_columns) == 1:
            self.generate_primary_keys(table, num_rows)
        elif len(pk_columns) > 1:
            self.generate_composite_primary_keys(table, num_rows)
        else:
            for _ in range(num_rows):
                self.generated_data[table].append({})

    def generate_primary_keys(self, table: str, num_rows: int):
        pk_columns = self.tables[table].get('primary_key', [])
        if len(pk_columns) != 1:
            return

        pk_col = pk_columns[0]
        col_info = self.get_column_info(table, pk_col)
        if not col_info:
            return

        col_type = col_info['type'].upper()
        if col_info.get("is_serial") or re.search(r'(INT|BIGINT|SMALLINT|DECIMAL|NUMERIC)', col_type):
            start_val = self.primary_keys[table][pk_col]
            values = np.arange(start_val, start_val + num_rows)
            new_rows = [{pk_col: int(value)} for value in values]
            self.primary_keys[table][pk_col] = start_val + num_rows
        else:
            # Non-numeric PK
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

    def generate_composite_primary_keys(self, table: str, num_rows: int):
        pk_columns = self.tables[table]['primary_key']
        pk_values = {}
        for pk in pk_columns:
            if self.is_foreign_key_column(table, pk):
                fk = next(
                    (fk for fk in self.tables[table]['foreign_keys'] if pk in fk['columns']),
                    None
                )
                if fk and fk['ref_table'] in self.generated_data:
                    ref_table = fk['ref_table']
                    ref_column = fk['ref_columns'][fk['columns'].index(pk)]
                    ref_data = self.generated_data[ref_table]
                    if ref_data:
                        pk_values[pk] = [r[ref_column] for r in ref_data]
                    else:
                        pk_values[pk] = [None]
                else:
                    pk_values[pk] = [None]
            else:
                col_info = self.get_column_info(table, pk)
                constraints = col_info.get('constraints', [])
                generated_list = []
                for _ in range(num_rows):
                    val = self.generate_column_value(table, col_info, {}, constraints)
                    generated_list.append(val)
                pk_values[pk] = generated_list

        combinations = list(set(itertools.product(*(pk_values[pk] for pk in pk_columns))))
        random.shuffle(combinations)
        max_possible_rows = len(combinations)
        if max_possible_rows < num_rows:
            logger.info(
                f"Not enough unique combos for composite PK in '{table}'. "
                f"Adjusting row count to {max_possible_rows}."
            )
            num_rows = max_possible_rows

        for i in range(num_rows):
            row = {}
            for idx, pk in enumerate(pk_columns):
                row[pk] = combinations[i][idx]
            self.generated_data[table].append(row)

    def enforce_constraints(self):
        """
        Enforce NOT NULL, CHECK, and UNIQUE constraints on each row in sequence.
        """
        for table in self.table_order:
            self.unique_values[table] = {}
            unique_constraints = self.tables[table].get('unique_constraints', []).copy()
            primary_key = self.tables[table].get('primary_key', [])
            if primary_key:
                unique_constraints.append(primary_key)
            for uniq_cols in unique_constraints:
                self.unique_values[table][tuple(uniq_cols)] = set()

            rows = self.generated_data[table]
            processed_rows = []
            for row in rows:
                new_row = self.process_row(table, row)
                self.enforce_unique_constraints(table, new_row)
                processed_rows.append(new_row)
            self.generated_data[table] = processed_rows

    def process_row(self, table: str, row: dict) -> dict:
        """
        Fill out a row (assign FK, fill columns, enforce constraints).
        """
        self.assign_foreign_keys(table, row)
        self.fill_remaining_columns(table, row)
        self.enforce_not_null_constraints(table, row)
        self.enforce_check_constraints(table, row)
        return row

    def assign_foreign_keys(self, table: str, row: dict):
        """
        Assign or fix foreign key values from the parent data.
        """
        fks = self.tables[table].get('foreign_keys', [])
        for fk in fks:
            fk_columns = fk['columns']
            ref_table = fk['ref_table']
            ref_columns = fk['ref_columns']

            child_values = [row.get(fc) for fc in fk_columns]
            all_set = all(v is not None for v in child_values)
            partially_set = any(v is not None for v in child_values) and not all_set

            parent_data = self.generated_data[ref_table]

            if all_set:
                # Check if those values exist in parent
                matching_parents = [
                    p for p in parent_data
                    if all(p[rc] == row[fc] for rc, fc in zip(ref_columns, fk_columns))
                ]
                if matching_parents:
                    continue
                else:
                    chosen_parent = random.choice(parent_data)
                    for rc, fc in zip(ref_columns, fk_columns):
                        row[fc] = chosen_parent[rc]
            elif partially_set:
                possible_parents = []
                for p in parent_data:
                    matches = True
                    for rc, fc in zip(ref_columns, fk_columns):
                        val_child = row.get(fc)
                        if val_child is not None and p[rc] != val_child:
                            matches = False
                            break
                    if matches:
                        possible_parents.append(p)
                if not possible_parents:
                    chosen_parent = random.choice(parent_data)
                else:
                    chosen_parent = random.choice(possible_parents)
                for rc, fc in zip(ref_columns, fk_columns):
                    if row.get(fc) is None:
                        row[fc] = chosen_parent[rc]
            else:
                # None are set
                chosen_parent = random.choice(parent_data)
                for rc, fc in zip(ref_columns, fk_columns):
                    row[fc] = chosen_parent[rc]

    def fill_remaining_columns(self, table: str, row: dict):
        """
        Populate remaining columns with synthetic data.
        """
        for column in self.tables[table]['columns']:
            col_name = column['name']
            if col_name in row:
                continue
            col_constraints = column.get('constraints', [])
            # Also check table-level constraints
            table_checks = self.tables[table].get('check_constraints', [])
            for constraint in table_checks:
                if col_name in constraint:
                    col_constraints.append(constraint)

            # If is_serial but not PK
            if column.get('is_serial'):
                if col_name not in self.primary_keys[table]:
                    self.primary_keys[table][col_name] = 1
                row[col_name] = self.primary_keys[table][col_name]
                self.primary_keys[table][col_name] += 1
            else:
                row[col_name] = self.generate_column_value(
                    table, column, row, constraints=col_constraints
                )

    def enforce_not_null_constraints(self, table: str, row: dict):
        """
        Ensure NOT NULL columns have a valid value.
        """
        for column in self.tables[table]['columns']:
            col_name = column['name']
            constraints = column.get('constraints', [])
            if 'NOT NULL' in constraints and row.get(col_name) is None:
                row[col_name] = self.generate_column_value(
                    table, column, row, constraints=constraints
                )

    def enforce_check_constraints(self, table: str, row: dict):
        """
        Evaluate all table-level check constraints until they're satisfied.
        """
        check_constraints = self.tables[table].get('check_constraints', [])
        for check in check_constraints:
            conditions = self.check_evaluator.extract_conditions(check)
            for col_name, conds in conditions.items():
                column = self.get_column_info(table, col_name)
                if column:
                    row[col_name] = self.generate_value_based_on_conditions(row, column, conds)

        # Recheck constraints in a loop until satisfied or some iteration limit
        max_attempts = 500
        attempts = 0
        while True:
            updates = {}
            for check in check_constraints:
                is_valid, candidate = self.check_evaluator.evaluate(check, row)
                if not is_valid:
                    # Generate a new candidate for columns in the failing check
                    conditions = self.check_evaluator.extract_conditions(check)
                    for col_name, _ in conditions.items():
                        updates[col_name] = candidate
            if not updates:
                break
            for col, new_val in updates.items():
                row[col] = new_val
            attempts += 1
            if attempts > max_attempts:
                logger.warning(f"Could not satisfy some CHECK constraints after {max_attempts} attempts.")
                break

    def is_foreign_key_column(self, table_p: str, col_name: str) -> bool:
        """
        Determine whether a specific column is part of a foreign key.
        """
        fks = self.tables[table_p].get('foreign_keys', [])
        for fk in fks:
            if col_name in fk['columns']:
                return True
        return False

    def enforce_unique_constraints(self, table: str, row: dict):
        """
        Ensure uniqueness for all UNIQUE constraints in a table.
        """
        unique_constraints = self.tables[table].get('unique_constraints', []).copy()
        for unique_cols in unique_constraints:
            unique_key = tuple(row[col] for col in unique_cols)
            unique_set = self.unique_values[table][tuple(unique_cols)]
            # Keep generating new values until the key is unique
            while unique_key in unique_set:
                for col in unique_cols:
                    if self.is_foreign_key_column(table, col):
                        continue
                    column = self.get_column_info(table, col)
                    row[col] = self.generate_column_value(table, column, row, constraints=unique_constraints)
                unique_key = tuple(row[col] for col in unique_cols)
            unique_set.add(unique_key)

    def generate_column_value(self, table: str, column: dict, row: dict, constraints=None):
        """
        Generate a synthetic value for a column, honoring constraints if possible.
        """
        constraints = constraints or []
        col_name = column['name']
        col_type = column['type'].upper()

        # Check for predefined values
        predefined_vals = None
        if table in self.predefined_values and col_name in self.predefined_values[table]:
            predefined_vals = self.predefined_values[table][col_name]
        elif 'global' in self.predefined_values and col_name in self.predefined_values['global']:
            predefined_vals = self.predefined_values['global'][col_name]
        if predefined_vals is not None:
            if isinstance(predefined_vals, list):
                return random.choice(predefined_vals)
            return predefined_vals

        # Check column type mappings
        mapping_entry = None
        if table in self.column_type_mappings and col_name in self.column_type_mappings[table]:
            mapping_entry = self.column_type_mappings[table][col_name]
        elif 'global' in self.column_type_mappings and col_name in self.column_type_mappings['global']:
            mapping_entry = self.column_type_mappings['global'][col_name]

        if mapping_entry:
            if callable(mapping_entry):
                return mapping_entry(self.fake, row)
            elif isinstance(mapping_entry, dict):
                gen_fn = mapping_entry.get('generator')
                if callable(gen_fn):
                    return gen_fn(self.fake, row)
                else:
                    return gen_fn
            else:
                return getattr(self.fake, mapping_entry)() if hasattr(self.fake, mapping_entry) else mapping_entry

        # Check regex constraints
        regex_patterns = extract_regex_pattern(constraints, col_name)
        if regex_patterns:
            pattern = regex_patterns[0]
            return generate_value_matching_regex(pattern)

        # Check for allowed values
        allowed_values = extract_allowed_values(constraints, col_name)
        if allowed_values:
            return random.choice(allowed_values)

        # Check numeric ranges
        numeric_ranges = extract_numeric_ranges(constraints, col_name)
        if numeric_ranges:
            return generate_numeric_value(numeric_ranges, col_type)

        return self.generate_value_based_on_type(col_type)

    def generate_value_based_on_conditions(self, row: dict, column: dict, conditions: list):
        """
        Generate a candidate value that satisfies a list of conditions.
        """
        col_type = column['type'].upper()
        # Check for direct equality condition
        for cond in conditions:
            if cond['operator'] in ('=', '=='):
                return cond['value']

        if re.search(r'(INT|INTEGER|SMALLINT|BIGINT|DECIMAL|NUMERIC|FLOAT|REAL)', col_type):
            lower_bound, upper_bound, epsilon = 1, 10000, 1
            if any(x in col_type for x in ['FLOAT', 'REAL', 'DECIMAL', 'NUMERIC']):
                lower_bound, upper_bound, epsilon = 1.0, 10000.0, 0.001

            for cond in conditions:
                op = cond['operator']
                val = cond['value']
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
            if lower_bound > upper_bound:
                return lower_bound
            if any(x in col_type for x in ['INT', 'INTEGER', 'SMALLINT', 'BIGINT']):
                return random.randint(int(lower_bound), int(upper_bound))
            else:
                return random.uniform(lower_bound, upper_bound)

        elif 'DATE' in col_type:
            default_lower, default_upper = date(1900, 1, 1), date.today()
            lb, ub = default_lower, default_upper
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
                    lb = max(lb, val + timedelta(days=1))
                elif op == '>=':
                    lb = max(lb, val)
                elif op == '<':
                    ub = min(ub, val - timedelta(days=1))
                elif op == '<=':
                    ub = min(ub, val)
            if lb > ub:
                return lb
            delta = (ub - lb).days
            return lb + timedelta(days=random.randint(0, delta))

        elif re.search(r'(CHAR|NCHAR|VARCHAR|NVARCHAR|TEXT)', col_type):
            for cond in conditions:
                if cond['operator'].upper() == 'LIKE':
                    pattern = cond['value'].strip('\'')
                    if pattern.endswith('%'):
                        fixed = pattern[:-1]
                        return fixed + ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=5))
                    elif pattern.startswith('%'):
                        fixed = pattern[1:]
                        return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=5)) + fixed
                    else:
                        return pattern

            length_match = re.search(r'\\((\\d+)\\)', col_type)
            length = int(length_match.group(1)) if length_match else 20
            return self.fake.lexify(text='?' * length)[:length]

        elif 'BOOL' in col_type:
            return random.choice([True, False])

        else:
            return self.generate_value_based_on_type(col_type)

    def generate_value_based_on_type(self, col_type: str):
        is_unsigned = False
        if col_type.startswith('U'):
            is_unsigned = True
            col_type = col_type[1:]
        col_type = col_type.upper()

        if re.match(r'.*(INT|INTEGER|SMALLINT|BIGINT).*', col_type):
            min_val = 0 if is_unsigned else -10000
            return int(np.random.randint(min_val, 10001))
        elif re.match(r'.*(DECIMAL|NUMERIC).*', col_type):
            precision, scale = 10, 2
            match = re.search(r'\\((\\d+),\\s*(\\d+)\\)', col_type)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
            max_value = 10 ** (precision - scale) - 1
            min_dec = 0.0 if is_unsigned else -9999.0
            return round(float(np.random.uniform(min_dec, max_value)), scale)
        elif re.match(r'.*(FLOAT|REAL|DOUBLE).*', col_type):
            return float(np.random.uniform(0, 10000))
        elif re.match(r'.*DATE.*', col_type):
            return self.fake.date_object()
        elif re.match(r'.*(TIMESTAMP|DATETIME).*', col_type):
            return self.fake.date_time()
        elif re.match(r'.*TIME.*', col_type):
            return self.fake.time()
        elif re.match(r'.*(CHAR|NCHAR|VARCHAR|NVARCHAR|TEXT).*', col_type):
            length_match = re.search(r'\\((\\d+)\\)', col_type)
            length = int(length_match.group(1)) if length_match else 255
            if length >= 5:
                return self.fake.text(max_nb_chars=length)[:length]
            elif length > 0:
                return self.fake.lexify(text='?' * length)
            else:
                return ''
        else:
            return self.fake.word()

    def get_column_info(self, table: str, col_name: str):
        """
        Retrieve column schema details from internal cache or table definition.
        """
        key = (table, col_name)
        if key not in self.column_info_cache:
            column_info = next(
                (col for col in self.tables[table]['columns'] if col['name'] == col_name),
                None
            )
            self.column_info_cache[key] = column_info
        return self.column_info_cache[key]

    def generate_data(self) -> dict:
        """
        Main entry point: generate initial data, then enforce constraints.
        """
        logger.info("Starting data generation.")
        self.generate_initial_data()
        logger.info("Initial data complete; enforcing constraints.")
        self.enforce_constraints()
        logger.info("Data generation finished.")
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
