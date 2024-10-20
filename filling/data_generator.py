import random
import re
from datetime import datetime, date, timedelta
from faker import Faker
from pyparsing import ParserElement
from .functions import generate_column_value, create_expression_parser, extract_columns_from_check

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
        self.fake = Faker()  # Initialize Faker as a class attribute
        self.table_order = self.resolve_table_order()
        self.initialize_primary_keys()
        self.expression_parser = create_expression_parser()

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

        Args:
            table (str): Table name.
            row (dict): Row data.
        """
        for column in self.tables[table]['columns']:
            col_name = column['name']
            if col_name in row:
                continue  # Value already set
            row[col_name] = generate_column_value(column, self.fake)

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
            max_attempts = 10000
            attempts = 0
            while not self.evaluate_check_constraint(check, row) and attempts < max_attempts:
                involved_columns = extract_columns_from_check(check)
                for col_name in involved_columns:
                    column = self.get_column_info(table, col_name)
                    if column:
                        row[col_name] = generate_column_value(column, self.fake)
                attempts += 1
            if attempts == max_attempts:
                raise ValueError(f"Unable to satisfy CHECK constraint '{check}' in table {table}")

    def evaluate_check_constraint(self, check, row):
        """
        Evaluate a CHECK constraint expression.

        Args:
            check (str): CHECK constraint expression.
            row (dict): Current row data.

        Returns:
            bool: True if the constraint is satisfied, False otherwise.
        """
        try:
            # Parse the expression
            parsed_expr = self.expression_parser.parseString(check, parseAll=True)[0]

            # Convert parsed expression to Python expression
            python_expr = self.convert_sql_expr_to_python(parsed_expr, row)

            # Evaluate the expression safely
            safe_globals = {
                '__builtins__': {},
                're': re,
                'datetime': datetime,
                'date': date,
                'timedelta': timedelta,
            }
            result = eval(python_expr, safe_globals, {})
            return bool(result)
        except Exception as e:
            # Log the exception for debugging
            print(f"Error evaluating check constraint: {e}")
            print(f"Constraint: {check}")
            return False

    def convert_sql_expr_to_python(self, parsed_expr, row):
        """
        Convert a parsed SQL expression into a Python expression.

        Args:
            parsed_expr: The parsed SQL expression.
            row (dict): Current row data.

        Returns:
            str: The Python expression.
        """
        if isinstance(parsed_expr, str):
            # It's a variable or a literal
            if parsed_expr.upper() == 'CURRENT_DATE':
                return f"datetime.now().date()"
            elif parsed_expr.upper() in ('TRUE', 'FALSE'):
                return parsed_expr.capitalize()
            elif parsed_expr in row:
                value = row[parsed_expr]
                if isinstance(value, datetime):
                    return f"datetime.strptime('{value.strftime('%Y-%m-%d %H:%M:%S')}', '%Y-%m-%d %H:%M:%S')"
                elif isinstance(value, date):
                    return f"datetime.strptime('{value.strftime('%Y-%m-%d')}', '%Y-%m-%d').date()"
                elif isinstance(value, str):
                    return f"'{value}'"
                else:
                    return str(value)
            elif re.match(r'^\d+(\.\d+)?$', parsed_expr):
                # It's a numeric literal
                return parsed_expr
            else:
                # Possibly a function name or unrecognized token
                return parsed_expr
        elif isinstance(parsed_expr, list):
            if len(parsed_expr) == 1:
                return self.convert_sql_expr_to_python(parsed_expr[0], row)
            else:
                # Handle function calls
                if isinstance(parsed_expr[0], str) and parsed_expr[1] == '(':
                    func_name = parsed_expr[0].upper()
                    args = parsed_expr[2:-1]  # Exclude opening and closing parentheses
                    args_expr = ', '.join(self.convert_sql_expr_to_python(arg, row) for arg in args)
                    # Map SQL functions to Python functions
                    func_map = {
                        'EXTRACT': 'lambda field, source: getattr(source, field.lower())',
                        # Add more function mappings as needed
                    }
                    if func_name in func_map:
                        return f"{func_map[func_name]}({args_expr})"
                # Handle unary NOT operator
                if parsed_expr[0].upper() == 'NOT':
                    operand = self.convert_sql_expr_to_python(parsed_expr[1], row)
                    return f"not ({operand})"
                # Handle binary operators
                left = self.convert_sql_expr_to_python(parsed_expr[0], row)
                operator = parsed_expr[1]
                right = self.convert_sql_expr_to_python(parsed_expr[2], row)
                # Map SQL operators to Python operators
                operator_map = {
                    '=': '==',
                    '<>': '!=',
                    '!=': '!=',
                    '>=': '>=',
                    '<=': '<=',
                    '>': '>',
                    '<': '<',
                    'AND': 'and',
                    'OR': 'or',
                    'LIKE': 're.match',
                    'NOT LIKE': 'not re.match',
                    'IS': 'is',
                    'IS NOT': 'is not',
                    'IN': 'in',
                    'NOT IN': 'not in',
                }
                python_operator = operator_map.get(operator.upper(), operator)
                if 'LIKE' in operator.upper():
                    # Handle LIKE operator using regex
                    pattern = right.strip("'").replace('%', '.*').replace('_', '.')
                    return f"{python_operator}('^{pattern}$', {left})"
                else:
                    return f"({left} {python_operator} {right})"
        else:
            # Handle literals (e.g., numbers)
            return str(parsed_expr)

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