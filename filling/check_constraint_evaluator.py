import re
from datetime import datetime, date

from pyparsing import (
    Word, alphas, alphanums, nums, oneOf, infixNotation, opAssoc,
    ParserElement, Keyword, QuotedString, Forward, Group, Suppress, Optional, delimitedList,
    ParseResults, Combine
)

ParserElement.enablePackrat()


class CheckConstraintEvaluator:
    """
    SQL CHECK Constraint Evaluator for Data Validation.

    The `CheckConstraintEvaluator` class is designed to parse and evaluate SQL CHECK constraints against row data. It ensures that all generated data adheres to the custom conditions defined within the database schema, maintaining data integrity and consistency.

    **Key Features:**
    - **Expression Parsing:** Utilizes Pyparsing to interpret complex SQL CHECK constraint expressions.
    - **Dynamic Evaluation:** Evaluates constraints dynamically against provided row data.
    - **Extensibility:** Easily extendable to support additional SQL functions and operators as needed.
    - **Error Handling:** Provides informative error messages for invalid or unsupported constraints.
    """

    def __init__(self, schema_columns=None):
        """
         Initialize the CheckConstraintEvaluator and set up the expression parser.

         Args:
             schema_columns (list, optional): List of all column names in the schema. Defaults to None.
         """
        self.expression_parser = self._create_expression_parser()
        self.schema_columns = schema_columns or []

    def _create_expression_parser(self):
        """
        Create and configure the Pyparsing expression parser for SQL CHECK constraints.

        This internal method sets up the grammar and parsing rules necessary to interpret SQL CHECK constraint expressions, including support for functions like EXTRACT and DATE.

        Returns:
            ParserElement: A configured Pyparsing parser element.
        """
        ParserElement.enablePackrat()

        # Basic elements
        integer = Word(nums)
        real = Combine(Word(nums) + '.' + Word(nums))
        number = real | integer
        string = QuotedString("'", escChar='\\', unquoteResults=False, multiline=True)
        identifier = Word(alphas, alphanums + "_$").setName("identifier")

        # Define operators
        arith_op = oneOf('+ - * /')
        comp_op = oneOf('= != <> < > <= >= IN NOT IN LIKE NOT LIKE IS IS NOT BETWEEN', caseless=True)
        bool_op = oneOf('AND OR', caseless=True)
        not_op = Keyword('NOT', caseless=True)

        lpar = Suppress('(')
        rpar = Suppress(')')

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

        # DATE function parsing
        date_func = Group(
            Keyword('DATE', caseless=True)('func_name') + lpar + expr('args') + rpar
        )

        # Atom can be a number, string, identifier, or function call
        atom = (
                extract_func | func_call | date_func | number | string | identifier | Group(lpar + expr + rpar)
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

    def extract_conditions(self, check_expression: str) -> dict:
        """
        Extract conditions from a CHECK constraint expression.

        Args:
            check_expression (str): The CHECK constraint expression to parse.

        Returns:
            dict: A dictionary mapping column names to their respective conditions extracted from the constraint.
        """
        try:
            parsed_expr = self.expression_parser.parseString(check_expression, parseAll=True)[0]
            conditions = self._extract_conditions_recursive(parsed_expr)
            return conditions
        except Exception as e:
            print(f"Error parsing check constraint: {e}")
            return {}

    def _extract_conditions_recursive(self, parsed_expr) -> dict:
        """
        Recursively extract conditions from the parsed SQL expression.

        Args:
            parsed_expr: The parsed SQL expression obtained from the expression parser.

        Returns:
            dict: A dictionary mapping column names to lists of condition dictionaries.
        """
        conditions = {}
        if isinstance(parsed_expr, ParseResults):
            if len(parsed_expr) == 3:
                left = parsed_expr[0]
                operator = parsed_expr[1].upper()
                right = parsed_expr[2]

                # Handle binary expressions
                if isinstance(left, str):
                    col_name = left
                    value = self._evaluate_literal(right, treat_as_identifier=True)
                    condition = {'operator': operator, 'value': value}
                    if col_name not in conditions:
                        conditions[col_name] = []
                    conditions[col_name].append(condition)
                else:
                    # Recurse on both sides
                    left_conditions = self._extract_conditions_recursive(left)
                    right_conditions = self._extract_conditions_recursive(right)
                    # Combine conditions
                    for col, conds in left_conditions.items():
                        conditions.setdefault(col, []).extend(conds)
                    for col, conds in right_conditions.items():
                        conditions.setdefault(col, []).extend(conds)
            elif len(parsed_expr) == 2:
                # Unary operator
                operator = parsed_expr[0].upper()
                operand = parsed_expr[1]
                operand_conditions = self._extract_conditions_recursive(operand)
                # Handle NOT operator
                if operator == 'NOT':
                    # Negate the conditions
                    for col, conds in operand_conditions.items():
                        for cond in conds:
                            cond['operator'] = 'NOT ' + cond['operator']
                    conditions.update(operand_conditions)
            else:
                # Recurse on each element
                for elem in parsed_expr:
                    elem_conditions = self._extract_conditions_recursive(elem)
                    for col, conds in elem_conditions.items():
                        conditions.setdefault(col, []).extend(conds)
        return conditions

    def _evaluate_literal(self, value, treat_as_identifier: bool = False):
        """
        Evaluate a literal value from the parsed expression, including function calls.

        Args:
            value: The parsed value to evaluate.
            treat_as_identifier (bool, optional): Whether to treat the value as an identifier (column name). Defaults to False.

        Returns:
            Any: The evaluated literal value.
        """
        if isinstance(value, ParseResults):
            if 'func_name' in value:
                func_name = value['func_name'].upper()
                if func_name == 'EXTRACT':
                    field = value['field']
                    source = self._evaluate_literal(value['source'])
                    return self.extract(field, source)
                elif func_name == 'DATE':
                    arg = self._evaluate_literal(value['args'])
                    return self.date_func(arg)
                else:
                    raise ValueError(f"Unsupported function '{func_name}' in CHECK constraint")
            else:
                # If it's a nested expression, evaluate it recursively
                return self._evaluate_literal(value[0], treat_as_identifier)
        elif isinstance(value, str):
            token = value.upper()
            if treat_as_identifier and (value in self.schema_columns or token in self.schema_columns):
                return value  # Return the column name as is
            elif token == 'CURRENT_DATE':
                return date.today()
            elif value.startswith("'") and value.endswith("'"):
                return value.strip("'")
            elif re.match(r'^\d+(\.\d+)?$', value):
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            else:
                return value  # Return the identifier or unrecognized token
        else:
            return value

    def extract_columns_from_check(self, check: str) -> list:
        """
        Extract column names from a CHECK constraint expression.

        Args:
            check (str): The CHECK constraint expression to analyze.

        Returns:
            list: A list of column names involved in the CHECK constraint.
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

    def evaluate(self, check_expression: str, row: dict) -> bool:
        """
        Evaluate a CHECK constraint expression against a row of data.

        Args:
            check_expression (str): The CHECK constraint expression to evaluate.
            row (dict): The row data to validate against the constraint.

        Returns:
            bool: True if the row satisfies the CHECK constraint, False otherwise.
        """
        try:
            parsed_expr = self.expression_parser.parseString(check_expression, parseAll=True)[0]
            result = self._evaluate_expression(parsed_expr, row)
            return bool(result)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Error evaluating check constraint: {e}")
            print(f"Constraint: {check_expression}")
            return False

    def _evaluate_expression(self, parsed_expr, row):
        """
        Recursively evaluate the parsed SQL expression against row data.

        Args:
            parsed_expr: The parsed SQL expression obtained from the expression parser.
            row (dict): The row data to evaluate the expression against.

        Returns:
            Any: The result of the expression evaluation.
        """
        if isinstance(parsed_expr, ParseResults):
            if 'func_name' in parsed_expr:
                func_name = parsed_expr['func_name'].upper()
                if func_name == 'EXTRACT':
                    field = parsed_expr['field']
                    source = self._evaluate_expression(parsed_expr['source'], row)
                    return self.extract(field, source)
                elif func_name == 'DATE':
                    args = self._evaluate_expression(parsed_expr['args'], row)
                    return self.date_func(args)
                else:
                    args = [self._evaluate_expression(arg, row) for arg in parsed_expr.get('args', [])]
                    func = getattr(self, func_name.lower(), None)
                    if func:
                        return func(*args)
                    else:
                        raise ValueError(f"Unsupported function '{func_name}' in CHECK constraint")
            elif len(parsed_expr) == 3:
                left = self._evaluate_expression(parsed_expr[0], row)
                operator = parsed_expr[1].upper()
                right = self._evaluate_expression(parsed_expr[2], row)
                return self.apply_operator(left, operator, right)
            elif len(parsed_expr) == 2:
                operator = parsed_expr[0].upper()
                operand = self._evaluate_expression(parsed_expr[1], row)
                if operator == 'NOT':
                    return not operand
                else:
                    raise ValueError(f"Unsupported unary operator '{operator}'")
            else:
                return self._evaluate_expression(parsed_expr[0], row)
        elif isinstance(parsed_expr, str):
            token = parsed_expr.upper()
            if token == 'CURRENT_DATE':
                return date.today()
            elif token in ('TRUE', 'FALSE'):
                return token == 'TRUE'
            elif parsed_expr in row:
                return row[parsed_expr]
            elif parsed_expr.startswith("'") and parsed_expr.endswith("'"):
                return parsed_expr.strip("'")
            elif re.match(r'^\d+(\.\d+)?$', parsed_expr):
                if '.' in parsed_expr:
                    return float(parsed_expr)
                else:
                    return int(parsed_expr)
            else:
                # Possibly an unrecognized token, treat as a string literal
                return parsed_expr
        else:
            return parsed_expr

    def date_func(self, arg) -> date:
        """
        Simulate the SQL DATE function.

        Args:
            arg: The argument to the DATE function, which can be a string or a datetime object.

        Returns:
            datetime.date: The date component extracted from the argument.

        Raises:
            ValueError: If the argument format is unsupported.
        """
        if isinstance(arg, str):
            return datetime.strptime(arg, '%Y-%m-%d').date()
        elif isinstance(arg, datetime):
            return arg.date()
        elif isinstance(arg, date):
            return arg
        else:
            raise ValueError(f"Unsupported argument for DATE function: {arg}")

    def apply_operator(self, left, operator: str, right):
        """
        Apply a binary operator to two operands.

        Args:
            left: The left operand in the operation.
            operator (str): The operator to apply (e.g., '=', '!=', '>', '<').
            right: The right operand in the operation.

        Returns:
            Any: The result of applying the operator to the operands.

        Raises:
            ValueError: If the operator is unsupported.
        """
        operator = operator.upper()
        if operator in ('=', '=='):
            return left == right
        elif operator in ('<>', '!='):
            return left != right
        elif operator == '>':
            return left > right
        elif operator == '<':
            return left < right
        elif operator == '>=':
            return left >= right
        elif operator == '<=':
            return left <= right
        elif operator == 'AND':
            return left and right
        elif operator == 'OR':
            return left or right
        elif operator == 'LIKE':
            return self.like(left, right)
        elif operator == 'NOT LIKE':
            return self.not_like(left, right)
        elif operator == 'IN':
            return left in right
        elif operator == 'NOT IN':
            return left not in right
        elif operator == 'IS':
            return left is right
        elif operator == 'IS NOT':
            return left is not right
        elif operator == '+':
            return left + right
        elif operator == '-':
            return left - right
        elif operator == '*':
            return left * right
        elif operator == '/':
            return left / right
        else:
            raise ValueError(f"Unsupported operator '{operator}'")

    def convert_sql_expr_to_python(self, parsed_expr, row: dict) -> str:
        """
        Convert a parsed SQL expression into a Python-compatible expression.

        Args:
            parsed_expr: The parsed SQL expression obtained from the expression parser.
            row (dict): The row data to reference during conversion.

        Returns:
            str: A string representing the Python expression equivalent of the SQL expression.
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

    def handle_operator(self, parsed_expr, row: dict) -> str:
        """
        Handle the conversion of parsed expressions containing operators to Python expressions.

        Args:
            parsed_expr: The parsed SQL expression containing operators.
            row (dict): The row data to reference during conversion.

        Returns:
            str: The converted Python expression.

        Raises:
            ValueError: If the operator is unsupported.
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

    def extract(self, field: str, source) -> int:
        """
        Simulate the SQL EXTRACT function to retrieve specific parts of a date.

        Args:
            field (str): The date field to extract (e.g., 'YEAR', 'MONTH', 'DAY').
            source (datetime.date or datetime.datetime): The date/time source from which to extract the field.

        Returns:
            int: The extracted value from the date field.

        Raises:
            ValueError: If the specified field is unsupported.
        """
        field = field.strip("'").lower()
        if isinstance(source, str):
            if source.upper() == 'CURRENT_DATE':
                source = date.today()
            else:
                try:
                    source = datetime.strptime(source, '%Y-%m-%d')
                except ValueError:
                    source = datetime.now()
        if isinstance(source, datetime):
            source = source.date()
        if field == 'year':
            return source.year
        elif field == 'month':
            return source.month
        elif field == 'day':
            return source.day
        else:
            raise ValueError(f"Unsupported field '{field}' for EXTRACT function")

    def regexp_like(self, value: str, pattern: str) -> bool:
        """
        Simulate the SQL REGEXP_LIKE function for pattern matching.

        Args:
            value (str): The string to test against the regex pattern.
            pattern (str): The regex pattern to match.

        Returns:
            bool: True if the value matches the pattern, False otherwise.
        """
        # Remove outer quotes from pattern if present
        if pattern.startswith("'") and pattern.endswith("'"):
            pattern = pattern[1:-1]
        if not isinstance(value, str):
            value = str(value)
        try:
            return re.match(pattern, value) is not None
        except re.error as e:
            print(f"Regex error: {e}")
            return False

    def like(self, value: str, pattern: str) -> bool:
        """
        Simulate the SQL LIKE operator using regex for pattern matching.

        Args:
            value (str): The string to match.
            pattern (str): The pattern, using SQL wildcards '%' and '_'.

        Returns:
            bool: True if the value matches the pattern, False otherwise.
        """
        pattern = pattern.strip("'").replace('%', '.*').replace('_', '.')
        if not isinstance(value, str):
            value = str(value)
        return re.match(f'^{pattern}$', value) is not None

    def not_like(self, value: str, pattern: str) -> bool:
        """
        Simulate the SQL NOT LIKE operator for pattern exclusion.

        Args:
            value (str): The string to test against the regex pattern.
            pattern (str): The pattern, using SQL wildcards '%' and '_'.

        Returns:
            bool: True if the value does not match the pattern, False otherwise.
        """
        return not self.like(value, pattern)
