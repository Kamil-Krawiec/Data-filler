from datetime import datetime, date

from faker import Faker

from .check_constraint_evaluator import CheckConstraintEvaluator
from .helpers import *

ParserElement.enablePackrat()


class DataGenerator:
    def __init__(self, tables, num_rows=10):
        """
        Initialize the DataGenerator with table schemas and the number of rows to generate.

        Args:
            tables (dict): Parsed table schemas.
            num_rows (int): Number of rows to generate per table.
        """
        self.tables = tables
        self.num_rows = num_rows
        self.generated_data = {}
        self.primary_keys = {}
        self.unique_values = {}
        self.predefined_values = {}
        self.fake = Faker()
        self.table_order = self.resolve_table_order()
        self.initialize_primary_keys()
        self.check_evaluator = CheckConstraintEvaluator()
        self.foreign_key_map = self.build_foreign_key_map()

    def build_foreign_key_map(self):
        """
        Build a mapping of foreign key relationships for quick lookup.

        Returns:
            dict: A mapping where each key is a parent table, and the value is a list of child table relationships.
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

    def resolve_table_order(self):
        """
        Resolve the order in which tables should be processed based on foreign key dependencies.

        Returns:
            list: Ordered list of table names.
        """
        order = []
        dependencies = {table: set() for table in self.tables}

        for table, details in self.tables.items():
            for fk in details.get('foreign_keys', []):
                if fk.get('ref_table'):
                    dependencies[table].add(fk['ref_table'])

        while dependencies:
            acyclic = False
            for table, deps in list(dependencies.items()):
                if not deps:
                    acyclic = True
                    order.append(table)
                    del dependencies[table]
                    for deps_set in dependencies.values():
                        deps_set.discard(table)
            if not acyclic:
                raise Exception("Circular dependency detected among tables.")

        return order

    def initialize_primary_keys(self):
        """
        Initialize primary key counters for each table.
        """
        for table in self.tables:
            self.primary_keys[table] = {}
            pk_columns = self.tables[table].get('primary_key', [])
            for pk in pk_columns:
                self.primary_keys[table][pk] = 1  # Start counting from 1

    def generate_initial_data(self):
        """
        Generate initial data for all tables without enforcing constraints.
        """
        for table in self.table_order:
            self.generated_data[table] = []
            for _ in range(self.num_rows):
                row = {}
                self.generate_primary_keys(table, row)
                self.generated_data[table].append(row)

    def generate_primary_keys(self, table, row):
        """
        Generate primary key values for a table row.

        Args:
            table (str): Table name.
            row (dict): Row data.
        """
        pk_columns = self.tables[table].get('primary_key', [])
        for pk in pk_columns:
            row[pk] = self.primary_keys[table][pk]
            self.primary_keys[table][pk] += 1

    def enforce_constraints(self):
        """
        Enforce all constraints on the generated data.
        """
        for table in self.table_order:
            self.unique_values[table] = {}
            unique_constraints = self.tables[table].get('unique_constraints', [])
            for unique_cols in unique_constraints:
                self.unique_values[table][tuple(unique_cols)] = set()

            for row in self.generated_data[table]:
                self.assign_foreign_keys(table, row)
                self.fill_remaining_columns(table, row)
                self.enforce_not_null_constraints(table, row)
                self.enforce_unique_constraints(table, row)
                self.enforce_check_constraints(table, row)

    def assign_foreign_keys(self, table, row):
        """
        Assign foreign key values to a table row.

        Args:
            table (str): Table name.
            row (dict): Row data.
        """
        fks = self.tables[table].get('foreign_keys', [])
        for fk in fks:
            fk_columns = fk['columns']
            ref_table = fk['ref_table']
            ref_columns = fk['ref_columns']
            if ref_table in self.generated_data and ref_columns:
                ref_record = random.choice(self.generated_data[ref_table])
                for idx, fk_col in enumerate(fk_columns):
                    ref_col = ref_columns[idx]
                    row[fk_col] = ref_record[ref_col]
            else:
                for fk_col in fk_columns:
                    row[fk_col] = None

    def fill_remaining_columns(self, table, row):
        """
        Fill in the remaining columns of a table row.
        """
        for column in self.tables[table]['columns']:
            col_name = column['name']
            if col_name in row:
                continue  # Value already set

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

            row[col_name] = generate_column_value(column, self.fake, constraints=col_constraints)

    def enforce_not_null_constraints(self, table, row):
        """
        Enforce NOT NULL constraints on a table row.

        Args:
            table (str): Table name.
            row (dict): Row data.
        """
        for column in self.tables[table]['columns']:
            col_name = column['name']
            constraints = column.get('constraints', [])
            if 'NOT NULL' in constraints and row.get(col_name) is None:
                row[col_name] = generate_column_value(column, self.fake)

    def enforce_unique_constraints(self, table, row):
        """
        Enforce unique constraints on a table row.

        Args:
            table (str): Table name.
            row (dict): Row data.
        """
        unique_constraints = self.tables[table].get('unique_constraints', [])
        for unique_cols in unique_constraints:
            unique_key = tuple(row[col] for col in unique_cols)
            unique_set = self.unique_values[table][tuple(unique_cols)]
            max_attempts = 100
            attempts = 0
            while unique_key in unique_set and attempts < max_attempts:
                for col in unique_cols:
                    column = self.get_column_info(table, col)
                    row[col] = generate_column_value(column, self.fake)
                unique_key = tuple(row[col] for col in unique_cols)
                attempts += 1
            unique_set.add(unique_key)
            if attempts == max_attempts:
                raise ValueError(f"Unable to generate unique value for columns {unique_cols} in table {table}")

    def enforce_check_constraints(self, table, row):
        """
        Enforce CHECK constraints on a table row.

        Args:
            table (str): Table name.
            row (dict): Row data.
        """
        check_constraints = self.tables[table].get('check_constraints', [])
        for check in check_constraints:
            max_attempts = 1000
            attempts = 0
            while not self.check_evaluator.evaluate(check, row) and attempts < max_attempts:
                involved_columns = self.check_evaluator.extract_columns_from_check(check)
                for col_name in involved_columns:
                    column = self.get_column_info(table, col_name)
                    if column:
                        row[col_name] = generate_column_value(column, self.fake)
                attempts += 1
            if attempts == max_attempts:
                raise ValueError(f"Unable to satisfy CHECK constraint '{check}' in table {table}")

    def get_column_info(self, table, col_name):
        """
        Get the column schema information for a specific column.

        Args:
            table (str): Table name.
            col_name (str): Column name.

        Returns:
            dict: Column schema.
        """
        for col in self.tables[table]['columns']:
            if col['name'] == col_name:
                return col
        return None

    def generate_data(self):
        """
        Generate the data by running all steps.

        Returns:
            dict: Generated data with constraints enforced.
        """
        self.generate_initial_data()
        self.enforce_constraints()
        self.repair_data()
        self.print_statistics()
        return self.generated_data

    def export_as_sql_insert_query(self):
        """
        Export the generated data as SQL INSERT queries.

        Returns:
            str: A string containing SQL INSERT queries.
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

    # Reparing the data

    def repair_data(self):
        """
        Iterate through the data and remove any rows that violate constraints,
        including cascading deletions to maintain referential integrity.
        """
        for table in self.table_order:
            self.repair_table_data(table)

    def repair_table_data(self, table):
        """
        Repair data for a specific table.

        Args:
            table (str): Table name.
        """
        original_row_count = len(self.generated_data[table])
        valid_rows = []
        deleted_rows = 0
        for row in self.generated_data[table]:
            if self.is_row_valid(table, row):
                valid_rows.append(row)
            else:
                deleted_rows += 1
                print(f"[Repair] Row deleted from table '{table}' due to constraint violation: {row}")
                # Remove dependent data in child tables
                self.remove_dependent_data(table, row)
        self.generated_data[table] = valid_rows
        if deleted_rows > 0:
            print(f"[Repair] Deleted {deleted_rows} row(s) from table '{table}' during repair.")
    def is_row_valid(self, table, row):
        """
        Check if a row is valid by checking all constraints.

        Args:
            table (str): Table name.
            row (dict): Row data.

        Returns:
            bool: True if the row is valid, False otherwise.
        """

        # Check NOT NULL constraints
        for column in self.tables[table]['columns']:
            col_name = column['name']
            constraints = column.get('constraints', [])
            if 'NOT NULL' in constraints and row.get(col_name) is None:
                return False

        # Check UNIQUE constraints
        unique_constraints = self.tables[table].get('unique_constraints', [])
        for unique_cols in unique_constraints:
            unique_key = tuple(row.get(col) for col in unique_cols)
            if unique_key is None or any(val is None for val in unique_key):
                return False
            # Note: Since we're repairing after data generation, we assume uniqueness has been enforced

        # Check CHECK constraints
        check_constraints = self.tables[table].get('check_constraints', [])
        for check in check_constraints:
            if not self.check_evaluator.evaluate(check, row):
                return False

        # All constraints passed
        return True

    def remove_dependent_data(self, table, row):
        """
        Recursively remove dependent rows in child tables.

        Args:
            table (str): Table name where the row is removed.
            row (dict): The row data that was removed.
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
        Print statistics about the generated data.
        """
        print("\nData Generation Statistics:")
        for table in self.table_order:
            row_count = len(self.generated_data.get(table, []))
            print(f"Table '{table}': {row_count} row(s) generated.")