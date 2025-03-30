import re
from datetime import datetime, date, timedelta
from pyparsing import (
    Word, alphanums, nums, oneOf, infixNotation, opAssoc,
    ParserElement, Keyword, QuotedString, Forward, Group, Suppress, Optional, delimitedList,
    ParseResults, Combine
)

# Enable packrat parsing once
ParserElement.enablePackrat()


class CheckConstraintEvaluator:
    """
    SQL CHECK Constraint Evaluator for Data Validation.

    This class parses and evaluates SQL CHECK constraints against row data.
    It supports functions like EXTRACT and DATE, various operators (including BETWEEN),
    and provides helper functions for operand unification.
    """

    def __init__(self, schema_columns=None):
        """
        Initialize the evaluator.

        Args:
            schema_columns (list, optional): List of column names in the schema.
        """
        self.expression_parser = self._create_expression_parser()
        self.schema_columns = schema_columns or []
        self.parsed_constraint_cache = {}

    def _create_expression_parser(self):
        """
        Create and configure the pyparsing parser for SQL CHECK constraints.
        """
        ParserElement.enablePackrat()

        # Basic elements
        integer = Word(nums)
        real = Combine(Word(nums) + '.' + Word(nums))
        number = real | integer
        string = QuotedString("'", escChar='\\', unquoteResults=False, multiline=True)
        identifier = Word(alphanums, alphanums + "_$").setName("identifier")

        # Define operators
        arith_op = oneOf('+ - * /')
        comp_op = oneOf('= != <> < > <= >= IN NOT IN LIKE NOT LIKE IS IS NOT BETWEEN', caseless=True)
        bool_op = oneOf('AND OR', caseless=True)
        not_op = Keyword('NOT', caseless=True)

        lpar = Suppress('(')
        rpar = Suppress(')')

        expr = Forward()

        # Function call parsing
        func_call = Group(identifier('func_name') + lpar + Optional(delimitedList(expr))('args') + rpar)

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

        # Atom: a number, string, identifier, function call, or parenthesized expression
        atom = extract_func | func_call | date_func | number | string | identifier | Group(lpar + expr + rpar)

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

    def _get_parsed_expression(self, check_expression: str):
        """Cache and return the parsed expression for a given check expression."""
        if check_expression not in self.parsed_constraint_cache:
            parsed_expr = self.expression_parser.parseString(check_expression, parseAll=True)[0]
            self.parsed_constraint_cache[check_expression] = parsed_expr
        return self.parsed_constraint_cache[check_expression]

    def extract_conditions(self, check_expression: str) -> dict:
        """
        Extract conditions from a CHECK constraint into a dictionary.

        Returns a dict mapping column names to lists of condition dictionaries.
        """
        try:
            parsed_expr = self._get_parsed_expression(check_expression)
            return self._extract_conditions_recursive(parsed_expr)
        except Exception as e:
            print(f"Error parsing check constraint: {e}")
            return {}

    def _extract_conditions_recursive(self, parsed_expr) -> dict:
        """Recursively extract conditions from parsed_expr."""
        conditions = {}
        if isinstance(parsed_expr, ParseResults):
            if len(parsed_expr) == 3:
                left = parsed_expr[0]
                operator = str(parsed_expr[1]).upper()
                right = parsed_expr[2]
                if isinstance(left, str):
                    col_name = left
                    value = self._evaluate_literal(right, treat_as_identifier=True)
                    conditions.setdefault(col_name, []).append({'operator': operator, 'value': value})
                else:
                    left_cond = self._extract_conditions_recursive(left)
                    right_cond = self._extract_conditions_recursive(right)
                    for col, conds in left_cond.items():
                        conditions.setdefault(col, []).extend(conds)
                    for col, conds in right_cond.items():
                        conditions.setdefault(col, []).extend(conds)
            elif len(parsed_expr) == 2:
                operator = str(parsed_expr[0]).upper()
                operand = parsed_expr[1]
                operand_cond = self._extract_conditions_recursive(operand)
                if operator == 'NOT':
                    for col, conds in operand_cond.items():
                        for cond in conds:
                            cond['operator'] = 'NOT ' + cond['operator']
                    conditions.update(operand_cond)
            else:
                for elem in parsed_expr:
                    sub_cond = self._extract_conditions_recursive(elem)
                    for col, conds in sub_cond.items():
                        conditions.setdefault(col, []).extend(conds)
        return conditions

    def _evaluate_literal(self, value, treat_as_identifier: bool = False):
        """
        Evaluate a literal value from an expression.
        This simplified version handles ParseResults, strings, and numbers.
        """
        if isinstance(value, (ParseResults, list)):
            return self._evaluate_expression(value, {})
        if isinstance(value, str):
            token = value.upper()
            if treat_as_identifier and (value in self.schema_columns or token in self.schema_columns):
                return value
            if token == 'CURRENT_DATE':
                return date.today()
            if value.startswith("'") and value.endswith("'"):
                return value.strip("'")
            if re.match(r'^\d+(\.\d+)?$', value):
                return float(value) if '.' in value else int(value)
            return value
        return value

    def extract_columns_from_check(self, check: str) -> list:
        """
        Extract column names from a CHECK constraint expression.
        """
        identifiers = []

        def identifier_action(tokens):
            identifiers.append(tokens[0])

        integer = Word(nums)
        real = Word(nums + ".")
        string = QuotedString("'", escChar='\\')
        identifier = Word(alphanums, alphanums + "_$").setName("identifier")
        identifier.addParseAction(identifier_action)

        arith_op = oneOf('+ - * /')
        comp_op = oneOf('= != <> < > <= >= IN NOT IN LIKE NOT LIKE IS IS NOT', caseless=True)
        bool_op = oneOf('AND OR', caseless=True)
        not_op = Keyword('NOT', caseless=True)

        lpar = Suppress('(')
        rpar = Suppress(')')

        expr = Forward()
        atom = real | integer | string | identifier | Group(lpar + expr + rpar)
        expr <<= infixNotation(atom, [
            (not_op, 1, opAssoc.RIGHT),
            (arith_op, 2, opAssoc.LEFT),
            (comp_op, 2, opAssoc.LEFT),
            (bool_op, 2, opAssoc.LEFT),
        ])
        try:
            expr.parseString(check, parseAll=True)
        except Exception:
            pass

        keywords = {'AND', 'OR', 'NOT', 'IN', 'LIKE', 'IS', 'NULL', 'BETWEEN',
                    'EXISTS', 'ALL', 'ANY', 'SOME', 'TRUE', 'FALSE', 'CURRENT_DATE'}
        operators = {'=', '!=', '<>', '<', '>', '<=', '>=', '+', '-', '*', '/', '%', 'IS', 'NOT'}
        return [token for token in set(identifiers) if token.upper() not in keywords and token not in operators]

    def evaluate(self, check_expression: str, row: dict) -> bool:
        """
        Evaluate a CHECK constraint expression against a row.
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

    def _flatten(self, expr):
        """
        Recursively flatten a nested expression (list or ParseResults) into a flat list of tokens.
        """
        from pyparsing import ParseResults
        if not isinstance(expr, (list, ParseResults)):
            return [expr]
        flat_list = []
        for item in expr:
            flat_list.extend(self._flatten(item))
        return flat_list

    def _handle_between(self, tokens_str: str, row: dict):
        """
        Look for a BETWEEN clause in the token string and, if found,
        re-parse the value, lower, and upper parts to evaluate them.
        Returns True if the BETWEEN condition holds, False if not,
        or None if no BETWEEN clause was detected.
        """
        # This regex captures three groups:
        #   group1: everything before the first BETWEEN keyword,
        #   group2: the expression after BETWEEN and before AND,
        #   group3: everything after the first AND.
        pattern = re.compile(r'(.+?)\s+BETWEEN\s+(.+?)\s+AND\s+(.+)', re.IGNORECASE)
        match = pattern.search(tokens_str)
        if match:
            value_str = match.group(1).strip()
            lower_str = match.group(2).strip()
            upper_str = match.group(3).strip()
            val = self._evaluate_expression(value_str, row)
            low = self._evaluate_expression(lower_str, row)
            high = self._evaluate_expression(upper_str, row)
            if 'date' in tokens_str.lower():
                try:
                    val = self.date_func(val)
                    low = self.date_func(low)
                    high = self.date_func(high)
                except ValueError:
                    return low <= val <= high
            return low <= val <= high
        return None

    def _is_plain_function(self, s: str) -> bool:
        """Return True if the string looks like a plain-text function call (e.g. 'EXTRACT YEAR CURRENT_DATE')."""
        tokens = s.strip().split()
        return bool(tokens) and tokens[0].upper() in ("EXTRACT", "DATE")

    def _evaluate_plain_function(self, s: str, row: dict):
        """
        Evaluate a plain-text function call such as:
          EXTRACT YEAR CURRENT_DATE
          DATE '2020-01-01'
        """
        tokens = s.strip().split()
        func = tokens[0].upper()
        if func == "EXTRACT":
            # Expected syntax: EXTRACT <field> <source>
            if len(tokens) >= 3:
                field = tokens[1]
                source_str = " ".join(tokens[2:])
                return self.extract(field, self._evaluate_expression(source_str, row))
        elif func == "DATE":
            # Expected syntax: DATE <arg>
            if len(tokens) >= 2:
                arg_str = " ".join(tokens[1:])
                return self.date_func(self._evaluate_expression(arg_str, row))
        raise ValueError(f"Unsupported plain function call: {s}")

    def _evaluate_function_call(self, expr, row: dict):
        """
        Evaluate a structured function call (ParseResults with a 'func_name').
        Supports functions such as EXTRACT, DATE, UPPER, LOWER, LENGTH, SUBSTRING, ROUND,
        ABS, COALESCE, POWER, MOD, TRIM, INITCAP, CONCAT, REGEXP_LIKE, etc.
        """
        func_name = str(expr['func_name']).upper()
        # Map structured functions to their handling
        if func_name == 'EXTRACT':
            field = self._evaluate_expression(expr['field'], row)
            source = self._evaluate_expression(expr['source'], row)
            return self.extract(field, source)
        elif func_name == 'DATE':
            arg = self._evaluate_expression(expr['args'], row)
            return self.date_func(arg)
        # For all other functions, fallback to the series of if/elif as before.
        elif func_name == 'UPPER':
            arg = self._evaluate_expression(expr['args'], row)
            return str(arg).upper()
        elif func_name == 'LOWER':
            arg = self._evaluate_expression(expr['args'], row)
            return str(arg).lower()
        elif func_name == 'LENGTH':
            arg = self._evaluate_expression(expr['args'], row)
            return len(arg) if arg is not None else 0
        elif func_name in ('SUBSTRING', 'SUBSTR'):
            args = [self._evaluate_expression(a, row) for a in expr.get('args', [])]
            if len(args) == 2:
                s, start = args
                return s[max(0, start - 1):]
            elif len(args) >= 3:
                s, start, length = args[0], args[1], args[2]
                return s[max(0, start - 1):max(0, start - 1) + length]
            else:
                raise ValueError(f"{func_name} requires at least 2 arguments")
        elif func_name == 'ROUND':
            args = [self._evaluate_expression(a, row) for a in expr.get('args', [])]
            if len(args) == 1:
                return round(args[0])
            elif len(args) >= 2:
                return round(args[0], int(args[1]))
            else:
                raise ValueError("ROUND requires at least one argument")
        elif func_name == 'ABS':
            arg = self._evaluate_expression(expr['args'], row)
            return abs(arg)
        elif func_name == 'COALESCE':
            args = [self._evaluate_expression(a, row) for a in expr.get('args', [])]
            for a in args:
                if a is not None:
                    return a
            return None
        elif func_name == 'POWER':
            args = [self._evaluate_expression(a, row) for a in expr.get('args', [])]
            if len(args) >= 2:
                return args[0] ** args[1]
            else:
                raise ValueError("POWER requires two arguments")
        elif func_name == 'MOD':
            args = [self._evaluate_expression(a, row) for a in expr.get('args', [])]
            if len(args) >= 2:
                return args[0] % args[1]
            else:
                raise ValueError("MOD requires two arguments")
        elif func_name == 'TRIM':
            arg = self._evaluate_expression(expr['args'], row)
            return arg.strip() if isinstance(arg, str) else arg
        elif func_name in ('INITCAP', 'PROPER'):
            arg = self._evaluate_expression(expr['args'], row)
            return arg.title() if isinstance(arg, str) else str(arg).title()
        elif func_name == 'CONCAT':
            args = [self._evaluate_expression(a, row) for a in expr.get('args', [])]
            return "".join(str(a) for a in args)
        elif func_name == 'REGEXP_LIKE':
            args = [self._evaluate_expression(a, row) for a in expr.get('args', [])]
            return self.regexp_like(*args)
        else:
            # Fallback: try to use a method with the lowercase func_name.
            args = [self._evaluate_expression(arg, row) for arg in expr.get('args', [])]
            func = getattr(self, func_name.lower(), None)
            if func:
                return func(*args)
            else:
                raise ValueError(f"Unsupported function '{func_name}' in CHECK constraint")

    def _evaluate_expression(self, expr, row: dict):
        """
        Recursively evaluate an expression (which can be a ParseResults, list, or literal)
        against the provided row.
        """
        # If expr is a string, check for plain-text function calls.
        if isinstance(expr, str):
            expr = expr.strip()
            if self._is_plain_function(expr):
                return self._evaluate_plain_function(expr, row)
        # If expr is a list or ParseResults, process its elements.
        if isinstance(expr, (list, ParseResults)):
            if isinstance(expr, ParseResults) and 'func_name' in expr:
                return self._evaluate_function_call(expr, row)
            if len(expr) == 1:
                return self._evaluate_expression(expr[0], row)
            flat = self._flatten(expr)
            tokens_str = " ".join(str(tok) for tok in flat)
            between_result = self._handle_between(tokens_str, row)
            if between_result is not None:
                return between_result
            if len(expr) == 3:
                left_val = self._evaluate_expression(expr[0], row)
                operator = str(expr[1]).upper()
                right_val = self._evaluate_expression(expr[2], row)
                return self.apply_operator(left_val, operator, right_val)
            if len(expr) == 2:
                operator = str(expr[0]).upper()
                operand = self._evaluate_expression(expr[1], row)
                if operator == 'NOT':
                    return not operand
                else:
                    raise ValueError(f"Unsupported unary operator '{operator}'")
            result = None
            for item in expr:
                result = self._evaluate_expression(item, row)
            return result
        if isinstance(expr, str):
            token = expr.upper()
            if token == 'CURRENT_DATE':
                return date.today()
            if token in ('TRUE', 'FALSE'):
                return token == 'TRUE'
            if expr in row:
                return row[expr]
            if expr.startswith("'") and expr.endswith("'"):
                return expr.strip("'")
            if re.match(r'^\d+(\.\d+)?$', expr):
                return float(expr) if '.' in expr else int(expr)
            return expr
        return expr

    def apply_operator(self, left, operator: str, right):
        """
        Apply a binary operator to the left and right operands.
        This simplified version uses a mapping to reduce repetitive code.
        """
        op = operator.upper()
        # For comparison operators, unify operands.
        if op in ('=', '==', '<>', '!=', '>', '<', '>=', '<='):
            left, right = self.unify_operands(left, right)
        op_map = {
            '=': lambda l, r: l == r,
            '==': lambda l, r: l == r,
            '<>': lambda l, r: l != r,
            '!=': lambda l, r: l != r,
            '>': lambda l, r: l > r,
            '<': lambda l, r: l < r,
            '>=': lambda l, r: l >= r,
            '<=': lambda l, r: l <= r,
            'AND': lambda l, r: bool(l) and bool(r),
            'OR': lambda l, r: bool(l) or bool(r),
            '+': lambda l, r: l + r,
            '-': lambda l, r: l - r,
            '*': lambda l, r: l * r,
            '/': lambda l, r: l / r,
        }
        if op in op_map:
            return op_map[op](left, right)
        elif op == 'LIKE':
            return self.like(left, right)
        elif op == 'NOT LIKE':
            return self.not_like(left, right)
        elif op == 'IN':
            return left in right
        elif op == 'NOT IN':
            return left not in right
        elif op == 'IS':
            return left is right
        elif op == 'IS NOT':
            return left is not right
        elif op == 'BETWEEN':
            if not (isinstance(right, list) and len(right) == 2):
                raise ValueError("BETWEEN operator expects right side as [lower, upper]")
            lower, upper = right
            lower, _ = self.unify_operands(lower, lower)
            upper, _ = self.unify_operands(upper, upper)
            left, _ = self.unify_operands(left, left)
            return lower <= left <= upper
        else:
            raise ValueError(f"Unsupported operator '{operator}'")

    def date_func(self, arg):
        """
        Simulate the SQL DATE function.
        """
        if isinstance(arg, str):
            return datetime.strptime(arg, '%Y-%m-%d').date()
        elif isinstance(arg, datetime):
            return arg.date()
        elif isinstance(arg, date):
            return arg
        else:
            raise ValueError(f"Unsupported argument for DATE function: {arg}")

    def _as_date(self, val):
        """
        If val looks like a date string in one of the formats, return a date object.
        Otherwise, return val.
        """
        if not isinstance(val, str):
            return val
        lit = val.strip("'")
        for fmt in ("%Y-%m-%d", "%m-%d-%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(lit, fmt).date()
            except ValueError:
                continue
        return val

    def _as_numeric(self, val):
        """Attempt to convert val to a number."""
        if isinstance(val, (int, float)):
            return val
        if isinstance(val, str) and re.match(r'^\d+(\.\d+)?$', val):
            return float(val) if '.' in val else int(val)
        return None

    def unify_operands(self, left, right):
        """
        Coerce both operands to date or numeric if possible.
        """
        left_d = self._as_date(left)
        right_d = self._as_date(right)
        if isinstance(left_d, date) and isinstance(right_d, date):
            return left_d, right_d
        left_n = self._as_numeric(left)
        right_n = self._as_numeric(right)
        if left_n is not None and right_n is not None:
            return left_n, right_n
        return left, right

    def convert_sql_expr_to_python(self, parsed_expr, row: dict) -> str:
        """
        Convert a parsed SQL expression into a Python expression string.
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
                return parsed_expr
            else:
                return f"'{parsed_expr}'"
        elif isinstance(parsed_expr, ParseResults):
            if 'func_name' in parsed_expr:
                func_name = str(parsed_expr['func_name']).upper()
                if func_name == 'EXTRACT':
                    field = self.convert_sql_expr_to_python(parsed_expr['field'], row)
                    source = self.convert_sql_expr_to_python(parsed_expr['source'], row)
                    return f"self.extract({field}, {source})"
                else:
                    args = parsed_expr.get('args', [])
                    args_expr = [self.convert_sql_expr_to_python(arg, row) for arg in args]
                    func_map = {'REGEXP_LIKE': 'self.regexp_like'}
                    if func_name in func_map:
                        return f"{func_map[func_name]}({', '.join(args_expr)})"
                    else:
                        raise ValueError(f"Unsupported function '{func_name}' in CHECK constraint")
            elif len(parsed_expr) == 1:
                return self.convert_sql_expr_to_python(parsed_expr[0], row)
            else:
                return self.handle_operator(parsed_expr, row)
        elif len(parsed_expr) == 1:
            return self.convert_sql_expr_to_python(parsed_expr[0], row)
        else:
            return self.handle_operator(parsed_expr, row)

    def handle_operator(self, parsed_expr, row: dict) -> str:
        """
        Convert an operator expression into a Python expression string.
        """
        if len(parsed_expr) == 2:
            operator = parsed_expr[0]
            operand = self.convert_sql_expr_to_python(parsed_expr[1], row)
            if str(operator).upper() == 'NOT':
                return f"not ({operand})"
            else:
                raise ValueError(f"Unsupported unary operator '{operator}'")
        elif len(parsed_expr) == 3:
            left = self.convert_sql_expr_to_python(parsed_expr[0], row)
            operator = str(parsed_expr[1]).upper()
            right = self.convert_sql_expr_to_python(parsed_expr[2], row)
            if operator in ('IS', 'IS NOT'):
                if right.strip() == 'None':
                    python_operator = 'is' if operator == 'IS' else 'is not'
                    return f"({left} {python_operator} {right})"
                else:
                    python_operator = '==' if operator == 'IS' else '!='
                    return f"({left} {python_operator} {right})"
            else:
                operator_map = {
                    '=': '==', '<>': '!=', '!=': '!=', '>=': '>=',
                    '<=': '<=', '>': '>', '<': '<', 'AND': 'and', 'OR': 'or',
                    'LIKE': 'self.like', 'NOT LIKE': 'self.not_like',
                    'IN': 'in', 'NOT IN': 'not in'
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
        Simulate the SQL EXTRACT function.
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
        Simulate the SQL REGEXP_LIKE function.
        """
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
        Simulate the SQL LIKE operator.
        """
        pattern = pattern.strip("'").replace('%', '.*').replace('_', '.')
        if not isinstance(value, str):
            value = str(value)
        return re.match(f'^{pattern}$', value) is not None

    def not_like(self, value: str, pattern: str) -> bool:
        """
        Simulate the SQL NOT LIKE operator.
        """
        return not self.like(value, pattern)
