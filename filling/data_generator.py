import itertools
import logging
import os
import re
import json
import csv
import random
import numpy as np
from datetime import datetime, date, timedelta
from faker import Faker
from concurrent.futures import ThreadPoolExecutor, as_completed
from .column_mappings_generator import ColumnMappingsGenerator
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

    def __init__(self,
                 tables,
                 num_rows=10,
                 predefined_values=None,
                 column_type_mappings=None,
                 num_rows_per_table=None,
                 max_attepts_to_generate_value=50,
                 guess_column_type_mappings = False,
                 threshold_for_guessing=0.8):
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
        self.max_attempts = max_attepts_to_generate_value

        if guess_column_type_mappings:
            self.column_type_mappings = ColumnMappingsGenerator(
                threshold=threshold_for_guessing
            ).generate(tables)

    # --------------------------------------------------------------------------
    # Schema / Dependency Setup
    # --------------------------------------------------------------------------

    def build_foreign_key_map(self) -> dict:
        """
        Construct a mapping of foreign key relationships between parent and child tables.
        """
        fk_map = {}
        for table_name, details in self.tables.items():
            for fk in details.get('foreign_keys', []):
                parent_table = fk['ref_table']
                child_table = table_name
                parent_cols = tuple(fk['ref_columns'])
                child_cols = tuple(fk['columns'])

                if parent_table not in fk_map:
                    fk_map[parent_table] = []

                fk_map[parent_table].append({
                    'child_table': child_table,
                    'parent_columns': parent_cols,
                    'child_columns': child_cols,
                })
        return fk_map

    def get_all_column_names(self) -> list:
        """
        Retrieve a comprehensive list of all column names across all tables.
        """
        columns = set()
        for table_def in self.tables.values():
            for col in table_def['columns']:
                columns.add(col['name'])
        return list(columns)

    def resolve_table_order(self) -> list:
        """
        Determine the order for processing tables based on foreign key dependencies.
        Ensures parent tables come before child tables.
        """
        dependencies = {table: set() for table in self.tables}

        for table_name, details in self.tables.items():
            for fk in details.get('foreign_keys', []):
                ref_table = fk.get('ref_table')
                if ref_table in self.tables:
                    dependencies[table_name].add(ref_table)

        table_order = []
        while dependencies:
            no_deps = [t for t, deps in dependencies.items() if not deps]
            if not no_deps:
                raise Exception(
                    "Circular dependency detected among tables. "
                    f"Remaining: {dependencies}"
                )
            for t in no_deps:
                table_order.append(t)
                del dependencies[t]
            for t, deps in dependencies.items():
                deps.difference_update(no_deps)

        return table_order

    def initialize_primary_keys(self):
        """
        Initialize primary key counters for each table to ensure unique ID generation.
        """
        for table in self.tables:
            self.primary_keys[table] = {}
            pk_cols = self.tables[table].get('primary_key', [])
            for pk in pk_cols:
                self.primary_keys[table][pk] = 1

    # --------------------------------------------------------------------------
    # Data Generation (Initial Rows)
    # --------------------------------------------------------------------------

    def compute_table_levels(self) -> dict:
        """
        Return a dict like {level_number: [tableA, tableB, ...], ...}
        that groups tables by 'level' in the foreign-key dependency graph.
        """
        # Start with the linear topological order you already have:
        topo_order = self.table_order  # or self.resolve_table_order()
        # 'levels' will hold {table: level_int}
        levels = {}
        for table in topo_order:
            fks = self.tables[table].get('foreign_keys', [])
            if not fks:
                levels[table] = 0
                continue
            parent_levels = []
            for fk in fks:
                parent_tab = fk['ref_table']
                parent_levels.append(levels.get(parent_tab, 0))
            levels[table] = max(parent_levels) + 1

        level_groups = {}
        for table, lvl in levels.items():
            level_groups.setdefault(lvl, []).append(table)
        return level_groups

    def generate_initial_data(self):
        """
        Generate initial data in parallel groups by table level.
        """
        level_groups = self.compute_table_levels()
        for level in sorted(level_groups.keys()):
            tables_at_level = level_groups[level]
            # For concurrency: run each table’s generation in a thread
            with ThreadPoolExecutor(max_workers=len(tables_at_level)) as executor:
                futures = {executor.submit(self._generate_table_initial_data, t): t
                           for t in tables_at_level}
                for future in as_completed(futures):
                    table = futures[future]
                    try:
                        future.result()
                        logger.info(f"Initial data generated for table '{table}' (level {level}).")
                    except Exception as e:
                        logger.error(f"Error generating data for table '{table}': {e}")

    def _generate_table_initial_data(self, table: str):
        """
        Generate initial data (PK columns, or empty rows if no PK) for one table.
        """
        self.generated_data[table] = []
        row_count = self.num_rows_per_table.get(table, self.num_rows)
        pk_cols = self.tables[table].get('primary_key', [])

        if len(pk_cols) == 1:
            self.generate_primary_keys(table, row_count)
        elif len(pk_cols) > 1:
            self.generate_composite_primary_keys(table, row_count)
        else:
            for _ in range(row_count):
                self.generated_data[table].append({})

    def generate_primary_keys(self, table: str, row_count: int):
        """
        Generate single-column primary keys for 'row_count' rows.
        """
        pk_cols = self.tables[table].get('primary_key', [])
        if len(pk_cols) != 1:
            return
        pk_col = pk_cols[0]
        col_info = self.get_column_info(table, pk_col)
        if not col_info:
            return

        col_type = col_info['type'].upper()
        if col_info.get("is_serial") or re.search(r'(INT|BIGINT|SMALLINT|DECIMAL|NUMERIC)', col_type):
            start_val = self.primary_keys[table][pk_col]
            values = np.arange(start_val, start_val + row_count)
            new_rows = [{pk_col: int(val)} for val in values]
            self.primary_keys[table][pk_col] = start_val + row_count
        else:
            # Non-numeric PK
            constraints = col_info.get('constraints', [])
            used = set()
            vals = []
            while len(vals) < row_count:
                tmp_val = self.generate_column_value(table, col_info, {}, constraints)
                if tmp_val not in used:
                    used.add(tmp_val)
                    vals.append(tmp_val)
            new_rows = [{pk_col: v} for v in vals]

        self.generated_data[table] = new_rows

    def generate_composite_primary_keys(self, table: str, row_count: int):
        """
        Generate composite PK values by combining columns in a Cartesian product.
        """
        pk_cols = self.tables[table]['primary_key']
        pk_values = {}

        for pk in pk_cols:
            if self.is_foreign_key_column(table, pk):
                fk = next(
                    (fk for fk in self.tables[table]['foreign_keys'] if pk in fk['columns']),
                    None
                )
                if fk and fk['ref_table'] in self.generated_data:
                    ref_table = fk['ref_table']
                    ref_col = fk['ref_columns'][fk['columns'].index(pk)]
                    ref_data = self.generated_data[ref_table]
                    pk_values[pk] = [r[ref_col] for r in ref_data] if ref_data else [None]
                else:
                    pk_values[pk] = [None]
            else:
                col_info = self.get_column_info(table, pk)
                constraints = col_info.get('constraints', [])
                out = []
                for _ in range(row_count):
                    val = self.generate_column_value(table, col_info, {}, constraints)
                    out.append(val)
                pk_values[pk] = out

        combos = list(set(itertools.product(*(pk_values[pk] for pk in pk_cols))))
        random.shuffle(combos)
        max_possible_rows = len(combos)
        if max_possible_rows < row_count:
            logger.info(
                f"Not enough unique combos for composite PK in '{table}'. "
                f"Adjusting rows to {max_possible_rows}."
            )
            row_count = max_possible_rows

        for i in range(row_count):
            row = {}
            for idx, pk in enumerate(pk_cols):
                row[pk] = combos[i][idx]
            self.generated_data[table].append(row)

    # --------------------------------------------------------------------------
    # Constraint Enforcement
    # --------------------------------------------------------------------------

    def enforce_constraints(self):
        """
        Enforce NOT NULL, CHECK, and UNIQUE constraints across all tables,
        *in parallel by table level*.
        """
        level_groups = self.compute_table_levels()

        for level in sorted(level_groups.keys()):
            tables_at_level = level_groups[level]
            # Constrain concurrency to as many worker threads as tables at this level
            with ThreadPoolExecutor(max_workers=len(tables_at_level)) as executor:
                # Launch each table’s constraints in its own thread
                futures = {executor.submit(self._enforce_constraints_for_table, t): t
                           for t in tables_at_level}
                for future in as_completed(futures):
                    tbl = futures[future]
                    try:
                        future.result()
                        logger.info(f"Constraints enforced for table '{tbl}' at level {level}.")
                    except Exception as e:
                        logger.error(f"Error enforcing constraints for table '{tbl}': {e}")

    def _enforce_constraints_for_table(self, table: str):
        """
        Run the existing constraint-enforcement logic (NOT NULL, CHECK, UNIQUE)
        for a single table, exactly as it was in the original enforce_constraints method.
        """
        # Set up unique constraints
        self.unique_values[table] = {}
        unique_constraints = self.tables[table].get('unique_constraints', []).copy()
        primary_key = self.tables[table].get('primary_key', [])
        if primary_key:
            unique_constraints.append(primary_key)
        for unique_cols in unique_constraints:
            self.unique_values[table][tuple(unique_cols)] = set()

        rows = self.generated_data[table]

        # Process each row (NOT NULL, CHECK, etc.)
        # Example: you might do concurrency at the row level as well,
        # but let's keep it single-threaded inside the table function:
        processed_rows = []
        for row in rows:
            row = self.process_row(table, row)  # includes foreign keys, fill columns, not null, check
            self.enforce_unique_constraints(table, row)  # do uniqueness per row
            processed_rows.append(row)

        self.generated_data[table] = processed_rows

    def process_row(self, table: str, row: dict) -> dict:
        """
        Fill out a row: assign FKs, fill columns, ensure NOT NULL, check constraints.
        """
        self.assign_foreign_keys(table, row)
        self.fill_remaining_columns(table, row)
        self.enforce_not_null_constraints(table, row)
        self.enforce_check_constraints(table, row)
        return row

    def is_foreign_key_column(self, table: str, col_name: str) -> bool:
        """
        Check if a column is part of a foreign key definition in 'table'.
        """
        fks = self.tables[table].get('foreign_keys', [])
        return any(col_name in fk['columns'] for fk in fks)

    # --------------------------------------------------------------------------
    # Foreign Key Assignment
    # --------------------------------------------------------------------------

    def assign_foreign_keys(self, table: str, row: dict):
        """
        Auto-assign foreign key values from the parent table data.
        """
        fks = self.tables[table].get('foreign_keys', [])
        for fk in fks:
            fk_cols = fk['columns']
            ref_table = fk['ref_table']
            ref_cols = fk['ref_columns']

            child_vals = [row.get(fc) for fc in fk_cols]
            all_set = all(v is not None for v in child_vals)
            partially_set = any(v is not None for v in child_vals) and not all_set

            parent_data = self.generated_data[ref_table]

            if all_set:
                # Validate existence
                matches = [
                    p for p in parent_data
                    if all(p[rc] == row[fc] for rc, fc in zip(ref_cols, fk_cols))
                ]
                if matches:
                    continue
                else:
                    chosen = random.choice(parent_data)
                    for rc, fc in zip(ref_cols, fk_cols):
                        row[fc] = chosen[rc]
            elif partially_set:
                candidates = []
                for p in parent_data:
                    match = True
                    for rc, fc in zip(ref_cols, fk_cols):
                        child_val = row.get(fc)
                        if child_val is not None and p[rc] != child_val:
                            match = False
                            break
                    if match:
                        candidates.append(p)
                if not candidates:
                    chosen = random.choice(parent_data)
                else:
                    chosen = random.choice(candidates)
                for rc, fc in zip(ref_cols, fk_cols):
                    if row.get(fc) is None:
                        row[fc] = chosen[rc]
            else:
                # None set
                chosen = random.choice(parent_data)
                for rc, fc in zip(ref_cols, fk_cols):
                    row[fc] = chosen[rc]

    # --------------------------------------------------------------------------
    # Column Filling
    # --------------------------------------------------------------------------

    def fill_remaining_columns(self, table: str, row: dict):
        """
        For each column not yet assigned, generate a synthetic value.
        """
        columns = self.tables[table]['columns']
        for col in columns:
            col_name = col['name']
            if col_name in row:
                continue

            col_constraints = col.get('constraints', [])
            # Table-level checks that mention this column
            table_checks = self.tables[table].get('check_constraints', [])
            for chk in table_checks:
                if col_name in chk:
                    col_constraints.append(chk)

            if col.get('is_serial'):
                # If it's a serial, auto-increment
                if col_name not in self.primary_keys[table]:
                    self.primary_keys[table][col_name] = 1
                row[col_name] = self.primary_keys[table][col_name]
                self.primary_keys[table][col_name] += 1
            else:
                row[col_name] = self.generate_column_value(
                    table, col, row, constraints=col_constraints
                )

    # --------------------------------------------------------------------------
    # NOT NULL Constraints
    # --------------------------------------------------------------------------

    def enforce_not_null_constraints(self, table: str, row: dict):
        """
        Ensure columns with 'NOT NULL' are populated.
        """
        for col in self.tables[table]['columns']:
            col_name = col['name']
            constraints = col.get('constraints', [])
            if 'NOT NULL' in constraints and row.get(col_name) is None:
                row[col_name] = self.generate_column_value(
                    table, col, row, constraints=constraints
                )

    # --------------------------------------------------------------------------
    # CHECK Constraints
    # --------------------------------------------------------------------------

    def enforce_check_constraints(self, table: str, row: dict):
        """
        Repeatedly generate new candidate values until all CHECK constraints pass.
        """
        checks = self.tables[table].get('check_constraints', [])
        # First pass: propose new candidates
        for check_expr in checks:
            conditions = self.check_evaluator.extract_conditions(check_expr)
            for col_name, conds in conditions.items():
                col_info = self.get_column_info(table, col_name)
                if col_info:
                    row[col_name] = self.generate_value_based_on_conditions(row, col_info, conds)

        # Next, loop until all constraints are satisfied or a certain threshold is met
        max_attempts = self.max_attempts
        attempts = 0
        while True:
            updates = {}
            for check_expr in checks:
                is_ok, candidate = self.check_evaluator.evaluate(check_expr, row)
                if not is_ok:
                    # Failing columns get a new candidate
                    conds = self.check_evaluator.extract_conditions(check_expr)
                    for c_name, _ in conds.items():
                        updates[c_name] = candidate

            if not updates:
                break
            for c_name, new_val in updates.items():
                row[c_name] = new_val
            attempts += 1
            if attempts >= max_attempts:
                logger.warning(f"Unable to satisfy some CHECK constraints after {max_attempts} attempts.")
                break

    def generate_value_based_on_conditions(self, row: dict, column: dict, conditions: list):
        """
        Generate a candidate that satisfies the given conditions for this column.
        """
        col_type = column['type'].upper()
        # Direct equality?
        for cond in conditions:
            if cond['operator'] in ('=', '=='):
                return cond['value']

        # Numeric
        if re.search(r'(INT|INTEGER|SMALLINT|BIGINT|DECIMAL|NUMERIC|FLOAT|REAL)', col_type):
            lb, ub, epsilon = 1, 10000, 1
            if any(x in col_type for x in ['FLOAT', 'REAL', 'DECIMAL', 'NUMERIC']):
                lb, ub, epsilon = 1.0, 10000.0, 0.001

            for cond in conditions:
                op = cond['operator']
                val = cond['value']
                if isinstance(val, str) and val in self.get_all_column_names():
                    if val in row:
                        val = row[val]
                    else:
                        continue
                if op == '>':
                    lb = max(lb, val + epsilon)
                elif op == '>=':
                    lb = max(lb, val)
                elif op == '<':
                    ub = min(ub, val - epsilon)
                elif op == '<=':
                    ub = min(ub, val)

            if lb > ub:
                return lb

            if any(x in col_type for x in ['INT', 'INTEGER', 'SMALLINT', 'BIGINT']):
                return random.randint(int(lb), int(ub))
            else:
                return random.uniform(lb, ub)

        # Date
        elif 'DATE' in col_type:
            default_l, default_u = date(1900, 1, 1), date.today()
            lb, ub = default_l, default_u
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

        # String
        elif re.search(r'(CHAR|NCHAR|VARCHAR|NVARCHAR|TEXT)', col_type):
            # Look for LIKE
            for cond in conditions:
                if cond['operator'].upper() == 'LIKE':
                    pattern = cond['value'].strip("'")
                    if pattern.endswith('%'):
                        fixed = pattern[:-1]
                        return fixed + ''.join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5))
                    elif pattern.startswith('%'):
                        fixed = pattern[1:]
                        return ''.join(random.choices("abcdefghijklmnopqrstuvwxyz", k=5)) + fixed
                    else:
                        return pattern

            match = re.search(r'\((\d+)\)', col_type)
            length = int(match.group(1)) if match else 20
            return self.fake.lexify(text='?' * length)[:length]

        # Boolean
        elif 'BOOL' in col_type:
            return random.choice([True, False])

        # Fallback
        else:
            return self.generate_value_based_on_type(col_type)

    # --------------------------------------------------------------------------
    # UNIQUE Constraints
    # --------------------------------------------------------------------------

    def enforce_unique_constraints(self, table: str, row: dict):
        """
        Ensure row satisfies any UNIQUE constraints for the given table.
        """
        unique_constrs = self.tables[table].get('unique_constraints', [])
        for cols in unique_constrs:
            key_tuple = tuple(row[c] for c in cols)
            uniq_set = self.unique_values[table][tuple(cols)]
            while key_tuple in uniq_set:
                # Generate new values for the columns in the constraint
                for col_name in cols:
                    if self.is_foreign_key_column(table, col_name):
                        continue
                    col_info = self.get_column_info(table, col_name)
                    row[col_name] = self.generate_column_value(
                        table, col_info, row, constraints=unique_constrs
                    )
                key_tuple = tuple(row[c] for c in cols)
            uniq_set.add(key_tuple)

    # --------------------------------------------------------------------------
    # Value Generation
    # --------------------------------------------------------------------------

    def generate_column_value(self, table: str, column: dict, row: dict, constraints=None):
        """
        Generate a synthetic value for 'column' in 'table' subject to constraints.
        """
        constraints = constraints or []
        col_name = column['name']
        col_type = column['type'].upper()

        # Check for predefined
        pre_vals = None
        if table in self.predefined_values and col_name in self.predefined_values[table]:
            pre_vals = self.predefined_values[table][col_name]
        elif 'global' in self.predefined_values and col_name in self.predefined_values['global']:
            pre_vals = self.predefined_values['global'][col_name]

        if pre_vals is not None:
            if isinstance(pre_vals, list):
                return random.choice(pre_vals)
            return pre_vals

        # Check for mappings
        mapping_entry = None
        if table in self.column_type_mappings and col_name in self.column_type_mappings[table]:
            mapping_entry = self.column_type_mappings[table][col_name]
        elif 'global' in self.column_type_mappings and col_name in self.column_type_mappings['global']:
            mapping_entry = self.column_type_mappings['global'][col_name]

        if mapping_entry:
            if callable(mapping_entry):
                return mapping_entry(self.fake, row)
            elif isinstance(mapping_entry, dict):
                gen = mapping_entry.get('generator')
                if callable(gen):
                    return gen(self.fake, row)
                return gen
            else:
                # Could be a Faker attribute or a fixed string
                return getattr(self.fake, mapping_entry)() if hasattr(self.fake, mapping_entry) else mapping_entry

        # Regex constraints
        regex_patts = extract_regex_pattern(constraints, col_name)
        if regex_patts:
            # Use the first pattern
            return generate_value_matching_regex(regex_patts[0])

        # Allowed values (IN)
        allowed = extract_allowed_values(constraints, col_name)
        if allowed:
            return random.choice(allowed)

        # Numeric range
        numeric_range = extract_numeric_ranges(constraints, col_name)
        if numeric_range:
            return generate_numeric_value(numeric_range, col_type)

        # Default fallback
        return self.generate_value_based_on_type(col_type)

    def generate_value_based_on_type(self, col_type: str):
        """
        Fallback generator for a column based on general type inference.
        """
        is_unsigned = False
        if col_type.startswith('U'):
            is_unsigned = True
            col_type = col_type[1:]
        col_type = col_type.upper()

        # Integers
        if re.match(r'.*\b(INT|INTEGER|SMALLINT|BIGINT)\b.*', col_type):
            min_val = 0 if is_unsigned else -10000
            return int(np.random.randint(min_val, 10001))

        # Decimal / Numeric
        elif re.match(r'.*\b(DECIMAL|NUMERIC)\b.*', col_type):
            precision, scale = 10, 2
            match = re.search(r'\((\d+),\s*(\d+)\)', col_type)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
            max_val = 10 ** (precision - scale) - 1
            min_dec = 0.0 if is_unsigned else -9999.0
            return round(float(np.random.uniform(min_dec, max_val)), scale)

        # Float / Double
        elif re.match(r'.*\b(FLOAT|REAL|DOUBLE)\b.*', col_type):
            return float(np.random.uniform(0, 10000))

        # Date / Time
        elif re.match(r'.*\b(DATE)\b.*', col_type):
            return self.fake.date_object()
        elif re.match(r'.*\b(TIMESTAMP|DATETIME)\b.*', col_type):
            return self.fake.date_time()
        elif re.match(r'.*\b(TIME)\b.*', col_type):
            return self.fake.time()

        elif re.match(r'^ENUM\(', col_type):
            # Parse the enum definition, e.g. ENUM('M','F','OTHER')
            enum_match = re.match(r"^ENUM\((.*)\)$", col_type)
            if enum_match:
                raw_vals = enum_match.group(1)  # e.g. "'M','F','OTHER'"
                # Split by commas and strip extra quotes/spaces
                choices = [val.strip().strip("'") for val in raw_vals.split(',')]
                return random.choice(choices)

        # Text / Char
        elif re.match(r'.*\b(CHAR|NCHAR|VARCHAR|NVARCHAR|CHARACTER VARYING|TEXT)\b.*', col_type):
            length_match = re.search(r'\((\d+)\)', col_type)
            length = int(length_match.group(1)) if length_match else 255
            if length >= 5:
                return self.fake.text(max_nb_chars=length)[:length]
            elif length > 0:
                return self.fake.lexify(text='?' * length)
            else:
                return ''

        # Fallback for other
        else:
            return self.fake.word()

    def get_column_info(self, table: str, col_name: str) -> dict:
        """
        Retrieve a column's info (cached).
        """
        key = (table, col_name)
        if key not in self.column_info_cache:
            col_info = next(
                (c for c in self.tables[table]['columns'] if c['name'] == col_name),
                None
            )
            self.column_info_cache[key] = col_info
        return self.column_info_cache[key]

    # --------------------------------------------------------------------------
    # Orchestration
    # --------------------------------------------------------------------------

    def generate_data(self, run_repair=True, print_stats=True) -> dict:
        """
        Main entry point for data generation.
        """
        logger.info("Starting data generation process.")
        logger.info("Generating initial data (PK fields, etc.)...")
        self.generate_initial_data()
        logger.info("Initial data done. Enforcing constraints...")
        self.enforce_constraints()
        logger.info("Constraints enforced.")

        logger.info("Data generation finished.")
        return self.generated_data

    # --------------------------------------------------------------------------
    # Export
    # --------------------------------------------------------------------------

    def export_as_sql_insert_query(self, max_rows_per_insert: int = 1000) -> str:
        """
        Export all generated data as SQL INSERT statements.
        """
        insert_queries = []
        for table_name, rows in self.generated_data.items():
            if not rows:
                continue
            columns = [c['name'] for c in self.tables[table_name]['columns']]
            prefix = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES"

            # Chunk to avoid huge single insert
            for i in range(0, len(rows), max_rows_per_insert):
                chunk = rows[i: i + max_rows_per_insert]
                vals_list = []
                for record in chunk:
                    row_vals = []
                    for col in columns:
                        v = record.get(col)
                        if v is None:
                            row_vals.append('NULL')
                        elif isinstance(v, str):
                            esc = v.replace("'", "''")
                            row_vals.append(f"'{esc}'")
                        elif isinstance(v, datetime):
                            row_vals.append(f"'{v.strftime('%Y-%m-%d %H:%M:%S')}'")
                        elif isinstance(v, date):
                            row_vals.append(f"'{v.strftime('%Y-%m-%d')}'")
                        elif isinstance(v, bool):
                            row_vals.append('TRUE' if v else 'FALSE')
                        else:
                            row_vals.append(str(v))

                    vals_list.append(f"({', '.join(row_vals)})")
                insert_query = f"{prefix}\n" + ",\n".join(vals_list) + ";"
                insert_queries.append(insert_query)

        return "\n\n".join(insert_queries)

    def export_data_files(self, output_dir: str, file_type='SQL'):
        """
        Export data as CSV/JSON per table or as a single .sql file. (No concurrency.)
        """
        file_type = file_type.upper()
        os.makedirs(output_dir, exist_ok=True)

        if file_type == 'SQL':
            sql_path = os.path.join(output_dir, "data_inserts.sql")
            with open(sql_path, mode="w", encoding="utf-8") as f:
                f.write(self.export_as_sql_insert_query())
            logger.info(f"Exported SQL data to {sql_path}")

        elif file_type in ('CSV', 'JSON'):
            for table_name, rows in self.generated_data.items():
                if not rows:
                    continue
                columns = [c['name'] for c in self.tables[table_name]['columns']]

                if file_type == 'CSV':
                    csv_path = os.path.join(output_dir, f"{table_name}.csv")
                    with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=columns)
                        writer.writeheader()
                        for r in rows:
                            writer.writerow({col: r.get(col, "") for col in columns})
                    logger.info(f"Exported CSV for '{table_name}' to {csv_path}")

                elif file_type == 'JSON':
                    json_path = os.path.join(output_dir, f"{table_name}.json")
                    with open(json_path, mode="w", encoding="utf-8") as f:
                        json.dump(rows, f, indent=2, default=str)
                    logger.info(f"Exported JSON for '{table_name}' to {json_path}")

    def preview_inferred_mappings(self, num_preview=10):
        """
        Generate a small preview (num_preview rows) of data for each table
        using the guessed column_type_mappings. Print out the sample rows to
        help the user evaluate if the mappings look correct.
        """
        # if there are no inferred mappings, just return
        if not self.column_type_mappings:
            print("No column_type_mappings found. Either user did not enable guessing or no columns matched.")
            return

        for table_name, col_map in self.column_type_mappings.items():
            print(f"\n=== Preview for table '{table_name}' ===")
            # For each table, generate num_preview rows using only the known mappings
            for i in range(num_preview):
                # We'll build a row dictionary from the mapped columns
                row_data = {}
                for col_name, generator_fn in col_map.items():
                    # Some mappings might be a string indicating a direct Faker method (e.g. 'email')
                    # or might be a lambda/callable.
                    if callable(generator_fn):
                        # If it's a lambda of type (fake, row) -> ...
                        row_data[col_name] = generator_fn(self.fake, row_data)
                    elif isinstance(generator_fn, str):
                        # If it's a direct Faker attribute name
                        row_data[col_name] = getattr(self.fake, generator_fn)()
                    else:
                        # If unknown type, fallback:
                        row_data[col_name] = None
                print(f"Sample row {i + 1}: {row_data}")