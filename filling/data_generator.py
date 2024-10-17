import random
import re
from faker import Faker
from datetime import datetime
from pyparsing import (Word, alphas, alphanums, nums, oneOf, infixNotation, opAssoc, ParserElement, Keyword, Literal,
                       QuotedString)

fake = Faker()
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
        self.predefined_values = {
            'Categories': {
                'category_name': ['Fiction', 'Non-fiction', 'Science', 'History', 'Biography', 'Fantasy', 'Mystery']
            },
            'Authors': {
                'sex': ['M', 'F']
            },
        }
        self.table_order = self.resolve_table_order()
        self.initialize_primary_keys()
        self.expression_parser = self.create_expression_parser()

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
                self.fill_predefined_values(table, row)
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

    def fill_predefined_values(self, table, row):
        """
        Fill predefined values in a table row if applicable.

        Args:
            table (str): Table name.
            row (dict): Row data.
        """
        predefined = self.predefined_values.get(table, {})
        for col_name, values in predefined.items():
            if col_name not in row:
                row[col_name] = random.choice(values)

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
            row[col_name] = self.generate_column_value(table, column, row)

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
                row[col_name] = self.generate_column_value(table, column, row)

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
            max_attempts = 10
            attempts = 0
            while unique_key in unique_set and attempts < max_attempts:
                for col in unique_cols:
                    column = self.get_column_info(table, col)
                    row[col] = self.generate_column_value(table, column, row)
                unique_key = tuple(row[col] for col in unique_cols)
                attempts += 1
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
            max_attempts = 10
            attempts = 0
            while not self.evaluate_check_constraint(check, row) and attempts < max_attempts:
                involved_columns = self.extract_columns_from_check(check)
                for col_name in involved_columns:
                    column = self.get_column_info(table, col_name)
                    if column:
                        row[col_name] = self.generate_column_value(table, column, row)
                attempts += 1

    def generate_column_value(self, table, column, row):
        """
        Generate a value for a column based on its type and constraints.

        Args:
            table (str): Table name.
            column (dict): Column schema.
            row (dict): Current row data.

        Returns:
            Any: Generated value.
        """
        col_name = column['name']
        col_type = column['type']
        sex_value = row.get('sex')

        if 'INT' in col_type or 'SERIAL' in col_type:
            return random.randint(1, 1000)
        elif 'VARCHAR' in col_type:
            length_match = re.search(r'\((\d+)\)', col_type)
            length = int(length_match.group(1)) if length_match else 20
            return self.generate_string_value(col_name, length, row)
        elif 'CHAR' in col_type:
            length_match = re.search(r'\((\d+)\)', col_type)
            length = int(length_match.group(1)) if length_match else 1
            return ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=length))
        elif 'DATE' in col_type:
            return self.generate_date_value(col_name)
        elif 'DECIMAL' in col_type or 'NUMERIC' in col_type:
            precision, scale = 10, 2
            match = re.search(r'\((\d+),\s*(\d+)\)', col_type)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
            return round(random.uniform(0, 1000), scale)
        else:
            return fake.word()

    def generate_string_value(self, col_name, length, row):
        """
        Generate a string value for a VARCHAR column.

        Args:
            col_name (str): Column name.
            length (int): Maximum length of the string.
            row (dict): Current row data.

        Returns:
            str: Generated string value.
        """
        if col_name.lower() == 'first_name':
            sex_value = row.get('sex')
            if sex_value == 'M':
                return fake.first_name_male()[:length]
            elif sex_value == 'F':
                return fake.first_name_female()[:length]
            else:
                return fake.first_name()[:length]
        elif col_name.lower() == 'last_name':
            return fake.last_name()[:length]
        elif col_name.lower() == 'email':
            first = row.get('first_name', fake.first_name())
            last = row.get('last_name', fake.last_name())
            email_domain = row.get('email_domain', fake.free_email_domain())
            return f"{first.lower()}.{last.lower()}@{email_domain}"[:length]
        elif col_name.lower() == 'isbn':
            return ''.join(random.choices('0123456789', k=13))
        else:
            return fake.word()[:length]

    def generate_date_value(self, col_name):
        """
        Generate a date value for a DATE column.

        Args:
            col_name (str): Column name.

        Returns:
            str: Generated date string in 'YYYY-MM-DD' format.
        """
        if col_name == 'birth_date':
            start_date = datetime(1940, 1, 1)
            end_date = datetime(2000, 12, 31)
            return fake.date_between(start_date=start_date, end_date=end_date).isoformat()
        elif col_name == 'registration_date':
            start_date = datetime(2010, 1, 1)
            end_date = datetime.now()
            return fake.date_between(start_date=start_date, end_date=end_date).isoformat()
        else:
            return fake.date()

    def create_expression_parser(self):
        """
        Create a parser for SQL expressions used in CHECK constraints.

        Returns:
            pyparsing.ParserElement: The parser for expressions.
        """
        integer = Word(nums)
        real = Word(nums + ".")
        string = QuotedString("'", escChar='\\')
        identifier = Word(alphas, alphanums + "_$").setName("identifier")

        # Define operators
        arith_op = oneOf('+ - * /')
        comp_op = oneOf('= != <> < > <= >= IN NOT IN LIKE NOT LIKE')
        bool_op = oneOf('AND OR')
        not_op = Keyword('NOT')

        # Define expressions
        expr = infixNotation(
            integer | real | string | identifier,
            [
                (not_op, 1, opAssoc.RIGHT),
                (arith_op, 2, opAssoc.LEFT),
                (comp_op, 2, opAssoc.LEFT),
                (bool_op, 2, opAssoc.LEFT),
            ]
        )

        return expr

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
            return bool(eval(python_expr))
        except Exception as e:
            # You might want to log the exception for debugging
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
            if parsed_expr in row:
                value = row[parsed_expr]
                if isinstance(value, str):
                    return f"'{value}'"
                else:
                    return str(value)
            else:
                return parsed_expr
        elif isinstance(parsed_expr, list):
            if len(parsed_expr) == 1:
                return self.convert_sql_expr_to_python(parsed_expr[0], row)
            else:
                # Handle unary NOT operator
                if parsed_expr[0] == 'NOT':
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
                    'AND': 'and',
                    'OR': 'or',
                    'LIKE': 'in',
                    'NOT LIKE': 'not in'
                    # Add more operators as needed
                }
                python_operator = operator_map.get(operator.upper(), operator)
                return f"({left} {python_operator} {right})"
        else:
            # Other types (e.g., numbers)
            return str(parsed_expr)

    def extract_columns_from_check(self, check):
        """
        Extract column names from a CHECK constraint expression.

        Args:
            check (str): CHECK constraint expression.

        Returns:
            list: List of column names.
        """
        # Use regex to find identifiers (variables)
        tokens = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', check)
        # Exclude SQL keywords and operators
        keywords = {'AND', 'OR', 'NOT', 'IN', 'LIKE', 'IS', 'NULL', 'BETWEEN', 'EXISTS', 'ALL', 'ANY', 'SOME'}
        operators = {'=', '!=', '<>', '<', '>', '<=', '>=', '+', '-', '*', '/', '%'}
        return [token for token in tokens if token.upper() not in keywords and token not in operators]

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
                        values.append(f"'{value.strftime('%Y-%m-%d')}'")
                    else:
                        values.append(str(value))
                values_str = f"({', '.join(values)})"
                values_list.append(values_str)

            # Combine the INSERT prefix and values
            insert_query = f"{insert_prefix}\n" + ",\n".join(values_list) + ";"
            insert_queries.append(insert_query)

        # Combine all INSERT queries into a single string
        return "\n\n".join(insert_queries)