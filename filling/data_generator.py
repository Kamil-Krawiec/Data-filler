import random
import re
from datetime import datetime, date
import exrex
from faker import Faker
from pyparsing import ParserElement

from .check_constraint_evaluator import CheckConstraintEvaluator

ParserElement.enablePackrat()


def extract_numeric_ranges(constraints, col_name):
    ranges = []
    for constraint in constraints:
        # Match patterns like 'column >= value' or 'column <= value'
        matches = re.findall(
            r"{}\s*(>=|<=|>|<|=)\s*(\d+(?:\.\d+)?)".format(col_name),
            constraint)
        for operator, value in matches:
            ranges.append((operator, float(value)))

        # Handle BETWEEN clauses
        between_matches = re.findall(
            r"{}\s+BETWEEN\s+(\d+(?:\.\d+)?)\s+AND\s+(\d+(?:\.\d+)?)".format(col_name),
            constraint, re.IGNORECASE)
        for lower, upper in between_matches:
            ranges.append(('>=', float(lower)))
            ranges.append(('<=', float(upper)))
    return ranges


def generate_numeric_value(ranges, col_type):
    min_value = None
    max_value = None
    for operator, value in ranges:
        if operator == '>':
            min_value = max(min_value or (value + 1), value + 1)
        elif operator == '>=':
            min_value = max(min_value or value, value)
        elif operator == '<':
            max_value = min(max_value or (value - 1), value - 1)
        elif operator == '<=':
            max_value = min(max_value or value, value)
        elif operator == '=':
            min_value = max_value = value

    if min_value is None:
        min_value = 0
    if max_value is None:
        max_value = min_value + 10000  # Arbitrary upper limit

    if 'INT' in col_type or 'DECIMAL' in col_type or 'NUMERIC' in col_type:
        return random.randint(int(min_value), int(max_value))
    else:
        return random.uniform(min_value, max_value)


def generate_value_matching_regex(pattern):
    # Handle escape sequences
    pattern = pattern.encode('utf-8').decode('unicode_escape')
    # Generate a matching string
    try:
        value = exrex.getone(pattern)
        return value
    except Exception as e:
        print(f"Error generating value for pattern '{pattern}': {e}")
        return ''


def extract_regex_pattern(constraints, col_name):
    patterns = []
    for constraint in constraints:
        matches = re.findall(
            r"REGEXP_LIKE\s*\(\s*{}\s*,\s*'([^']+)'\s*\)".format(col_name),
            constraint, re.IGNORECASE)
        patterns.extend(matches)
    return patterns


def extract_allowed_values(constraints, col_name):
    allowed_values = []
    for constraint in constraints:
        match = re.search(
            r"{}\s+IN\s*\(([^)]+)\)".format(col_name),
            constraint, re.IGNORECASE)
        if match:
            values = match.group(1)
            # Split values and strip quotes
            values = [v.strip().strip("'") for v in values.split(',')]
            allowed_values.extend(values)
    return allowed_values


def generate_column_value(column, fake, constraints=None):
    constraints = constraints or []
    col_name = column['name']
    col_type = column['type'].upper()

    # PREDEFINED VALUES:
    if col_name in ['sex']:
        return random.choice(['M', 'F'])
    if col_name in ['name', 'first_name', 'firstname']:
        sex = column.get('sex', None)
        return fake.first_name_male() if sex == 'M' else fake.first_name_female()
    elif col_name in ['surname', 'last_name', 'lastname']:
        sex = column.get('sex', None)
        return fake.last_name_male() if sex == 'M' else fake.last_name_female()
    elif col_name == 'email':
        name = column.get('first_name', fake.word())
        surname = column.get('last_name', fake.word())
        return f"{name}.{surname}@{fake.free_email_domain()}"
    elif col_name == 'phone':
        return fake.phone_number()
    elif col_name == 'address':
        return fake.address()
    elif col_name == 'city':
        return fake.city()
    elif col_name == 'country':
        return fake.country()
    elif col_name == 'company':
        return fake.company()
    elif col_name == 'job_title':
        # Use predefined values or Faker
        return fake.job()

    # Check for regex constraints
    regex_patterns = extract_regex_pattern(constraints, col_name)
    if regex_patterns:
        # For simplicity, use the first pattern
        pattern = regex_patterns[0]
        return generate_value_matching_regex(pattern)

    # Check for allowed values (IN constraints)
    allowed_values = extract_allowed_values(constraints, col_name)
    if allowed_values:
        return select_allowed_value(allowed_values)

    # Check for numeric ranges
    numeric_ranges = extract_numeric_ranges(constraints, col_name)
    if numeric_ranges:
        return generate_numeric_value(numeric_ranges, col_type)

    # Map SQL data types to generic types
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
        return fake.date()
    elif re.match(r'.*\b(TIMESTAMP|DATETIME)\b.*', col_type):
        return fake.date_time()
    elif re.match(r'.*\b(TIME)\b.*', col_type):
        return fake.time()
    elif re.match(r'.*\b(CHAR|NCHAR|VARCHAR|NVARCHAR|CHARACTER VARYING|TEXT)\b.*', col_type):
        length_match = re.search(r'\((\d+)\)', col_type)
        length = int(length_match.group(1)) if length_match else 255
        if length >= 5:
            # Use fake.text for lengths >= 5
            return fake.text(max_nb_chars=length)[:length]
        elif length > 0:
            # Use fake.lexify for lengths < 5
            return fake.lexify(text='?' * length)
        else:
            # Length is zero or negative; return an empty string
            return ''
    else:
        # Default to text for unknown types
        return fake.word()


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
