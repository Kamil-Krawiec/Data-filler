from datetime import datetime, date, timedelta
import itertools
from faker import Faker
from .check_constraint_evaluator import CheckConstraintEvaluator
from .helpers import *

ParserElement.enablePackrat()


class DataGenerator:
    """
    A class to generate synthetic data for database tables based on provided schemas and constraints.
    """
    def __init__(self, tables, num_rows=10, predefined_values=None, column_type_mappings=None,
                 num_rows_per_table=None):
        """
        Initialize the DataGenerator with table schemas and the number of rows to generate.

        Args:
            tables (dict): Parsed table schemas.
            num_rows (int): Default number of rows to generate per table.
            predefined_values (dict): Dictionary of predefined values for specific columns.
            column_type_mappings (dict): Dictionary mapping column names to faker functions or data types.
            num_rows_per_table (dict): Dictionary specifying the number of rows for each table.
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

    def get_all_column_names(self):
        columns = set()
        for table in self.tables.values():
            for column in table['columns']:
                columns.add(column['name'])
        return list(columns)

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
            num_rows = self.num_rows_per_table.get(table, self.num_rows)
            pk_columns = self.tables[table].get('primary_key', [])
            if len(pk_columns) == 1:
                for _ in range(num_rows):
                    row = {}
                    self.generate_primary_keys(table, row)
                    self.generated_data[table].append(row)
            else:
                # Composite primary key; generate combinations
                self.generate_composite_primary_keys(table, num_rows)

    def generate_composite_primary_keys(self, table, num_rows):
        """
        Generate data for a table with a composite primary key.

        Args:
            table (str): Table name.
            num_rows (int): Number of rows to generate.
        """
        pk_columns = self.tables[table]['primary_key']

        # Generate possible values for each primary key column
        pk_values = {}
        for pk in pk_columns:
            # If the primary key column is a foreign key, get values from the referenced table
            if self.is_foreign_key_column(table, pk):
                fk = next((fk for fk in self.tables[table]['foreign_keys'] if pk in fk['columns']), None)
                ref_table = fk['ref_table']
                ref_column = fk['ref_columns'][fk['columns'].index(pk)]
                pk_values[pk] = [row[ref_column] for row in self.generated_data[ref_table]]
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
                # Skip assigning if FK columns are already set (e.g., in composite PKs)
                if all(col in row for col in fk_columns):
                    continue
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

            # Generate the column value
            row[col_name] = self.generate_column_value(table, column, row, constraints=col_constraints)

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
                row[col_name] = self.generate_column_value(table, column, row, constraints=constraints)

    def generate_column_value(self, table, column, row, constraints=None):
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

        # Default data generation based on column type
        return self.generate_value_based_on_type(col_type)

    def generate_value_based_on_type(self, col_type):
        """
        Generate a value based on the SQL data type.

        Args:
            col_type (str): Column data type.

        Returns:
            Any: Generated value.
        """
        if re.match(r'.*\b(INT|INTEGER|SMALLINT|BIGINT)\b.*', col_type):
            return random.randint(1, 10000)
        elif re.match(r'.*\b(DECIMAL|NUMERIC)\b.*', col_type):
            # Handle decimal and numeric types with precision and scale
            precision, scale = 10, 2  # Default values
            match = re.search(r'\((\d+),\s*(\d+)\)', col_type)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
            max_value = 10 ** (precision - scale) - 1
            return round(random.uniform(0, max_value), scale)
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

    def is_foreign_key_column(self, table_p, col_name):
        """
        Check if a column is a foreign key in the specified table.

        Args:
            table_p (str): Table name.
            col_name (str): Column name.

        Returns:
            bool: True if the column is a foreign key, False otherwise.
        """
        fks = self.tables[table_p].get('foreign_keys', [])
        for fk in fks:
            if col_name in fk['columns']:
                return True
        return False

    def enforce_unique_constraints(self, table, row):
        """
        Enforce unique constraints on a table row.
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

    def enforce_check_constraints(self, table, row):
        """
        Enforce CHECK constraints on a table row.

        Args:
            table (str): Table name.
            row (dict): Row data.
        """
        check_constraints = self.tables[table].get('check_constraints', [])
        for check in check_constraints:
            conditions = self.check_evaluator.extract_conditions(check)
            while not self.check_evaluator.evaluate(check, row):
                for col_name, conds in conditions.items():
                    column = self.get_column_info(table, col_name)
                    if column:
                        row[col_name] = self.generate_value_based_on_conditions(row, column, conds)

    def generate_value_based_on_conditions(self, row, column, conditions):

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

    def is_row_valid(self, table, row):
        """
        Check if a row is valid by checking all constraints.

        Args:
            table (str): Table name.
            row (dict): Row data.

        Returns:
            tuple: (is_valid, violated_constraint)
                is_valid (bool): True if the row is valid, False otherwise.
                violated_constraint (str): Description of the violated constraint, or None if valid.
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
