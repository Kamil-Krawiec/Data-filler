import re
from datetime import datetime, date, timedelta

from pyparsing import (
    Word, alphas, alphanums, nums, oneOf, infixNotation, opAssoc,
    ParserElement, Keyword, QuotedString, Forward, Group, Suppress, Optional, delimitedList,
    ParseResults
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
        ParserElement.enablePackrat()

        integer = Word(nums)
        real = Word(nums + ".")
        string = QuotedString("'", escChar='\\', unquoteResults=False, multiline=True)
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
            identifier('func_name') + lpar + Optional(delimitedList(expr))('args') + rpar
        )

        # EXTRACT function parsing
        extract_func = Group(
            Keyword('EXTRACT', caseless=True)('func_name') + lpar +
            (identifier | string)('field') +
            Keyword('FROM', caseless=True).suppress() +
            expr('source') + rpar
        )

        # Atom can be an identifier, number, string, or a function call
        atom = (
                extract_func | func_call | real | integer | string | identifier | Group(lpar + expr + rpar)
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
            print(f"Parsed Expression: {parsed_expr}")

            # Convert parsed expression to Python expression
            python_expr = self.convert_sql_expr_to_python(parsed_expr, row)
            print(f"Python Expression: {python_expr}")

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
            # Log the exception with detailed traceback
            import traceback
            traceback.print_exc()
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
            elif parsed_expr.startswith("'") and parsed_expr.endswith("'"):
                # It's a string literal with quotes preserved
                return parsed_expr
            else:
                # Possibly an unrecognized token, treat as a string literal
                return f"'{parsed_expr}'"
        if isinstance(parsed_expr, str):
            # ... [existing code remains unchanged]
            pass  # For brevity
        elif isinstance(parsed_expr, ParseResults):
            if 'func_name' in parsed_expr:
                func_name = parsed_expr['func_name'].upper()
                if func_name == 'EXTRACT':
                    # Handle EXTRACT function with 'field' and 'source'
                    field = self.convert_sql_expr_to_python(parsed_expr['field'], row)
                    source = self.convert_sql_expr_to_python(parsed_expr['source'], row)
                    return f"self.extract({field}, {source})"
                else:
                    # Handle other function calls
                    args = parsed_expr.get('args', [])
                    args_expr = [self.convert_sql_expr_to_python(arg, row) for arg in args]
                    func_map = {
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
        elif len(parsed_expr) == 1:
            return self.convert_sql_expr_to_python(parsed_expr[0], row)
        else:
            # Handle unary and binary operators
            return self.handle_operator(parsed_expr, row)

    def handle_operator(self, parsed_expr, row):
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
            operator = parsed_expr[1].upper()
            right = self.convert_sql_expr_to_python(parsed_expr[2], row)

            if operator in ('IS', 'IS NOT'):
                # Determine if the right operand is NULL
                if right.strip() == 'None':
                    # Use 'is' or 'is not' when comparing to NULL (None)
                    python_operator = 'is' if operator == 'IS' else 'is not'
                    return f"({left} {python_operator} {right})"
                else:
                    # Use '==' or '!=' for other comparisons
                    python_operator = '==' if operator == 'IS' else '!='
                    return f"({left} {python_operator} {right})"
            else:
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
                    'IN': 'in',
                    'NOT IN': 'not in',
                }
                python_operator = operator_map.get(operator)
                if python_operator is None:
                    raise ValueError(f"Unsupported operator '{operator}'")
                if 'LIKE' in operator:
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
        # Remove outer quotes from pattern if present
        if pattern.startswith("'") and pattern.endswith("'"):
            pattern = pattern[1:-1]
        # Handle escape sequences
        pattern = pattern.encode('utf-8').decode('unicode_escape')
        # Ensure value is a string
        if not isinstance(value, str):
            value = str(value)
        try:
            return re.match(pattern, value) is not None
        except re.error as e:
            print(f"Regex error: {e}")
            return False

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
