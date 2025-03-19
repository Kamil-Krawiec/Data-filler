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
        Evaluate a literal value from parsed expression.
        """
        if isinstance(value, ParseResults):
            if 'func_name' in value:
                func_name = str(value['func_name']).upper()
                if func_name == 'EXTRACT':
                    field = value['field']
                    source = self._evaluate_literal(value['source'])
                    return self.extract(field, source)
                elif func_name == 'DATE':
                    arg = self._evaluate_literal(value['args'])
                    return self.date_func(arg)
                else:
                    raise ValueError(f"Unsupported function '{func_name}' in CHECK constraint")
            return self._evaluate_literal(value[0], treat_as_identifier)
        elif isinstance(value, str):
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

    def _evaluate_expression(self, parsed_expr, row):
        """
        Recursively evaluate parsed_expr against row.
        First, flatten the expression. If a BETWEEN pattern [left, 'BETWEEN', lower, 'AND', upper]
        is found in the flat list, evaluate that pattern. Otherwise, evaluate as a binary/unary expression.
        """
        tokens = self._flatten(parsed_expr)
        upper_tokens = [str(tok).upper() for tok in tokens]

        if "BETWEEN" in upper_tokens and "AND" in upper_tokens:
            try:
                between_idx = upper_tokens.index("BETWEEN")
                and_idx = upper_tokens.index("AND", between_idx + 1)
                if between_idx == 0:
                    raise ValueError("Missing expression before BETWEEN")
                value_expr = tokens[between_idx - 1]
                lower_expr = tokens[between_idx + 1]
                if and_idx + 1 >= len(tokens):
                    raise ValueError("Missing expression after AND in BETWEEN clause")
                upper_expr = tokens[and_idx + 1]
                val = self._evaluate_single_token(value_expr, row)
                low = self._evaluate_single_token(lower_expr, row)
                high = self._evaluate_single_token(upper_expr, row)
                return (low <= val <= high)
            except Exception as e:
                # Fall back if BETWEEN pattern not correctly parsed.
                pass

        if isinstance(parsed_expr, ParseResults):
            if 'func_name' in parsed_expr:
                func_name = str(parsed_expr['func_name']).upper()
                if func_name == 'EXTRACT':
                    field = parsed_expr['field']
                    source = self._evaluate_expression(parsed_expr['source'], row)
                    return self.extract(field, source)
                elif func_name == 'DATE':
                    arg = self._evaluate_expression(parsed_expr['args'], row)
                    return self.date_func(arg)
                else:
                    args = [self._evaluate_expression(a, row) for a in parsed_expr.get('args', [])]
                    func = getattr(self, func_name.lower(), None)
                    if func:
                        return func(*args)
                    raise ValueError(f"Unsupported function '{func_name}' in CHECK constraint")
            if len(parsed_expr) == 3:
                left_val = self._evaluate_expression(parsed_expr[0], row)
                operator = str(parsed_expr[1]).upper()
                right_val = self._evaluate_expression(parsed_expr[2], row)
                return self.apply_operator(left_val, operator, right_val)
            if len(parsed_expr) == 2:
                op = str(parsed_expr[0]).upper()
                operand = self._evaluate_expression(parsed_expr[1], row)
                if op == 'NOT':
                    return not operand
                raise ValueError(f"Unsupported unary operator '{op}'")
            if len(parsed_expr) == 1:
                return self._evaluate_expression(parsed_expr[0], row)
            result = None
            for item in parsed_expr:
                result = self._evaluate_expression(item, row)
            return result

        if isinstance(parsed_expr, list):
            result = None
            for item in parsed_expr:
                result = self._evaluate_expression(item, row)
            return result

        if isinstance(parsed_expr, str):
            token = parsed_expr.upper()
            if token == 'CURRENT_DATE':
                return date.today()
            if token in ('TRUE', 'FALSE'):
                return token == 'TRUE'
            if parsed_expr in row:
                return row[parsed_expr]
            if parsed_expr.startswith("'") and parsed_expr.endswith("'"):
                return parsed_expr.strip("'")
            if re.match(r'^\d+(\.\d+)?$', parsed_expr):
                return float(parsed_expr) if '.' in parsed_expr else int(parsed_expr)
            return parsed_expr

        return parsed_expr

    def _evaluate_single_token(self, token, row):
        """
        Evaluate a single token (literal, column reference, or simple expression).
        """
        from pyparsing import ParseResults
        if isinstance(token, ParseResults) and 'func_name' in token:
            func_name = str(token['func_name']).upper()
            if func_name == 'EXTRACT':
                field = token['field']
                source = self._evaluate_single_token(token['source'], row)
                return self.extract(field, source)
            elif func_name == 'DATE':
                arg = self._evaluate_single_token(token['args'], row)
                return self.date_func(arg)
            else:
                raise ValueError(f"Unsupported function '{func_name}' in CHECK constraint")
        if isinstance(token, (list, ParseResults)):
            return self._evaluate_expression(token, row)
        if isinstance(token, str):
            upper_token = token.upper()
            if upper_token == 'CURRENT_DATE':
                return date.today()
            if upper_token in ('TRUE', 'FALSE'):
                return upper_token == 'TRUE'
            if token in row:
                return row[token]
            if token.startswith("'") and token.endswith("'"):
                return token.strip("'")
            if re.match(r'^\d+(\.\d+)?$', token):
                return float(token) if '.' in token else int(token)
            return token
        return token

    def apply_operator(self, left, operator: str, right):
        """
        Apply a binary operator. For comparison operators, unify operands.
        """
        operator = operator.upper()
        if operator in ('=', '==', '<>', '!=', '>', '<', '>=', '<='):
            left, right = self.unify_operands(left, right)
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
            return bool(left) and bool(right)
        elif operator == 'OR':
            return bool(left) or bool(right)
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
        elif operator == 'BETWEEN':
            if not (isinstance(right, list) and len(right) == 2):
                raise ValueError("BETWEEN operator expects right side as [lower, upper]")
            lower, upper = right
            lower, _ = self.unify_operands(lower, lower)
            upper, _ = self.unify_operands(upper, upper)
            left, _ = self.unify_operands(left, left)
            return lower <= left <= upper
        else:
            raise ValueError(f"Unsupported operator '{operator}'")

    def date_func(self, arg) -> date:
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