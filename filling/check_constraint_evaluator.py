import re
from datetime import datetime, date, timedelta
from pyparsing import (
    ParseResults, Word, alphas, alphanums, nums, oneOf, infixNotation, opAssoc,
    ParserElement, Keyword, QuotedString, Literal, Forward, Group, ZeroOrMore, Suppress, Optional
)

ParserElement.enablePackrat()


class CheckConstraintEvaluator:
    def __init__(self):
        self.expression_parser = self._create_expression_parser()

    def _create_expression_parser(self):
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
        comp_op = oneOf('= != <> < > <= >= IN NOT IN LIKE NOT LIKE IS IS NOT', caseless=True)
        bool_op = oneOf('AND OR', caseless=True)
        not_op = Keyword('NOT', caseless=True)

        lpar = Suppress('(')
        rpar = Suppress(')')
        comma = Suppress(',')

        expr = Forward()

        # Function call parsing
        func_call = Group(
            identifier('func_name') + lpar + Optional(Group(expr + ZeroOrMore(comma + expr)))('args') + rpar
        )

        # Atom can be an identifier, number, string, or a function call
        atom = (
            func_call | real | integer | string | identifier | Group(lpar + expr + rpar)
        )

        # Define expressions using infix notation
        expr <<= infixNotation(
            atom,
            [
                (not_op, 1, opAssoc.RIGHT),
                (arith_op, 2, opAssoc.LEFT),
                (comp_op, 2, opAssoc.LEFT),
                (bool_op, 2, opAssoc.LEFT),
            ]
        )

        return expr

    def extract_columns_from_check(self, check):
        """
        Extract column names from a CHECK constraint expression.

        Args:
            check (str): CHECK constraint expression.

        Returns:
            list: List of column names.
        """
        identifiers = []

        def identifier_action(tokens):
            identifiers.append(tokens[0])

        # Define grammar components
        integer = Word(nums)
        real = Word(nums + ".")
        string = QuotedString("'", escChar='\\')
        identifier = Word(alphas, alphanums + "_$").setName("identifier")
        identifier.addParseAction(identifier_action)

        # Define operators
        arith_op = oneOf('+ - * /')
        comp_op = oneOf('= != <> < > <= >= IN NOT IN LIKE NOT LIKE IS IS NOT', caseless=True)
        bool_op = oneOf('AND OR', caseless=True)
        not_op = Keyword('NOT', caseless=True)

        lpar = Suppress('(')
        rpar = Suppress(')')

        expr = Forward()
        atom = (
            real | integer | string | identifier | Group(lpar + expr + rpar)
        )

        expr <<= infixNotation(
            atom,
            [
                (not_op, 1, opAssoc.RIGHT),
                (arith_op, 2, opAssoc.LEFT),
                (comp_op, 2, opAssoc.LEFT),
                (bool_op, 2, opAssoc.LEFT),
            ]
        )

        try:
            expr.parseString(check, parseAll=True)
        except Exception:
            pass  # Ignore parsing errors for now

        # Remove duplicates and SQL keywords/operators
        keywords = {'AND', 'OR', 'NOT', 'IN', 'LIKE', 'IS', 'NULL', 'BETWEEN',
                    'EXISTS', 'ALL', 'ANY', 'SOME', 'TRUE', 'FALSE', 'CURRENT_DATE'}
        operators = {'=', '!=', '<>', '<', '>', '<=', '>=', '+', '-', '*', '/', '%', 'IS', 'NOT'}
        columns = [token for token in set(identifiers) if token.upper() not in keywords and token not in operators]
        return columns

    def evaluate(self, check_expression, row):
        """
        Evaluate a CHECK constraint expression.

        Args:
            check_expression (str): CHECK constraint expression.
            row (dict): Current row data.

        Returns:
            bool: True if the constraint is satisfied, False otherwise.
        """
        try:
            # Parse the expression
            parsed_expr = self.expression_parser.parseString(check_expression, parseAll=True)[0]

            # Convert parsed expression to Python expression
            python_expr = self.convert_sql_expr_to_python(parsed_expr, row)

            # Evaluate the expression safely
            safe_globals = {
                '__builtins__': {},
                're': re,
                'datetime': datetime,
                'date': date,
                'timedelta': timedelta,
                'self': self,  # Allow access to class methods
            }
            result = eval(python_expr, safe_globals, {})
            return bool(result)
        except Exception as e:
            # Log the exception for debugging
            print(f"Error evaluating check constraint: {e}")
            print(f"Constraint: {check_expression}")
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
            token = parsed_expr.upper()
            if token == 'CURRENT_DATE':
                return "datetime.now().date()"
            elif token in ('TRUE', 'FALSE'):
                return token.capitalize()
            elif parsed_expr in row:
                value = row[parsed_expr]
                if isinstance(value, datetime):
                    return f"datetime.strptime('{value.strftime('%Y-%m-%d %H:%M:%S')}', '%Y-%m-%d %H:%M:%S')"
                elif isinstance(value, date):
                    return f"datetime.strptime('{value.strftime('%Y-%m-%d')}', '%Y-%m-%d').date()"
                elif isinstance(value, str):
                    escaped_value = value.replace("'", "\\'")
                    return f"'{escaped_value}'"
                else:
                    return str(value)
            elif re.match(r'^\d+(\.\d+)?$', parsed_expr):
                return parsed_expr
            else:
                # Possibly a function name or unrecognized token
                return parsed_expr
        elif isinstance(parsed_expr, ParseResults):
            if 'func_name' in parsed_expr:
                # Handle function calls
                func_name = parsed_expr['func_name'].upper()
                args = parsed_expr.get('args', [])
                args_expr = [self.convert_sql_expr_to_python(arg, row) for arg in args]
                # Map SQL functions to Python functions
                func_map = {
                    'EXTRACT': 'self.extract',
                    'REGEXP_LIKE': 'self.regexp_like',
                    # Add more function mappings as needed
                }
                if func_name in func_map:
                    return f"{func_map[func_name]}({', '.join(args_expr)})"
                else:
                    raise ValueError(f"Unsupported function '{func_name}' in CHECK constraint")
            elif len(parsed_expr) == 1:
                return self.convert_sql_expr_to_python(parsed_expr[0], row)
            else:
                # Handle unary and binary operators
                return self.handle_operator(parsed_expr, row)
        else:
            # Handle literals (e.g., numbers)
            return str(parsed_expr)

    def handle_operator(self, parsed_expr, row):
        """
        Handle unary and binary operators in the parsed expression.

        Args:
            parsed_expr: The parsed expression containing operators.
            row (dict): Current row data.

        Returns:
            str: The Python expression.
        """
        if len(parsed_expr) == 2:
            # Unary operator
            operator = parsed_expr[0]
            operand = self.convert_sql_expr_to_python(parsed_expr[1], row)
            if operator.upper() == 'NOT':
                return f"not ({operand})"
            else:
                raise ValueError(f"Unsupported unary operator '{operator}'")
        elif len(parsed_expr) == 3:
            # Binary operator
            left = self.convert_sql_expr_to_python(parsed_expr[0], row)
            operator = parsed_expr[1]
            right = self.convert_sql_expr_to_python(parsed_expr[2], row)
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
                'LIKE': 'self.like',
                'NOT LIKE': 'self.not_like',
                'IS': 'is',
                'IS NOT': 'is not',
                'IN': 'in',
                'NOT IN': 'not in',
            }
            python_operator = operator_map.get(operator.upper(), operator)
            if 'LIKE' in operator.upper():
                return f"{python_operator}({left}, {right})"
            else:
                return f"({left} {python_operator} {right})"
        else:
            raise ValueError(f"Unsupported expression structure: {parsed_expr}")

    def extract(self, field, source):
        """
        Simulate SQL EXTRACT function.

        Args:
            field (str): Field to extract (e.g., 'YEAR').
            source (datetime.date or datetime.datetime): Date/time source.

        Returns:
            int: Extracted value.
        """
        field = field.strip("'").lower()
        if isinstance(source, str):
            # Attempt to parse the date string
            try:
                source = datetime.strptime(source, '%Y-%m-%d')
            except ValueError:
                source = datetime.now()
        if field == 'year':
            return source.year
        elif field == 'month':
            return source.month
        elif field == 'day':
            return source.day
        else:
            raise ValueError(f"Unsupported field '{field}' for EXTRACT function")

    def regexp_like(self, value, pattern):
        """
        Simulate SQL REGEXP_LIKE function.

        Args:
            value (str): The string to test.
            pattern (str): The regex pattern.

        Returns:
            bool: True if the value matches the pattern.
        """
        # Remove quotes from pattern if present
        pattern = pattern.strip("'")
        # Escape backslashes in the pattern
        pattern = pattern.encode('unicode_escape').decode().replace('\\\\', '\\')
        # Ensure value is a string
        if not isinstance(value, str):
            value = str(value)
        return re.match(pattern, value) is not None

    def like(self, value, pattern):
        """
        Simulate SQL LIKE operator using regex.

        Args:
            value (str): The string to match.
            pattern (str): The pattern, with SQL wildcards.

        Returns:
            bool: True if the value matches the pattern.
        """
        pattern = pattern.strip("'").replace('%', '.*').replace('_', '.')
        # Ensure value is a string
        if not isinstance(value, str):
            value = str(value)
        return re.match(f'^{pattern}$', value) is not None

    def not_like(self, value, pattern):
        """
        Simulate SQL NOT LIKE operator.

        Args:
            value (str): The string to match.
            pattern (str): The pattern, with SQL wildcards.

        Returns:
            bool: True if the value does not match the pattern.
        """
        return not self.like(value, pattern)