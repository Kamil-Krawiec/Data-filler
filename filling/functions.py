import random
import re

from pyparsing import (
    Word, alphas, alphanums, nums, oneOf, infixNotation, opAssoc, Keyword, QuotedString, Group, Forward
)


def generate_column_value(column, fake):
    """
    Generate a value for a column based on its type and constraints.

    Args:
        column (dict): Column schema.

    Returns:
        Any: Generated value.
    """
    col_type = column['type'].upper()

    # Map SQL data types to generic types
    if re.match(r'.*\b(INT|INTEGER|SMALLINT|BIGINT)\b.*', col_type):
        return random.randint(1, 10000)
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
    elif re.match(r'.*\b(DATE)\b.*', col_type):
        return fake.date()
    elif re.match(r'.*\b(TIMESTAMP|DATETIME)\b.*', col_type):
        return fake.date_time()
    elif re.match(r'.*\b(TIME)\b.*', col_type):
        return fake.time()
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
    else:
        # Default to text for unknown types
        return fake.word()


def create_expression_parser():
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
    comp_op = oneOf('= != <> < > <= >= IN NOT IN LIKE NOT LIKE IS IS NOT')
    bool_op = oneOf('AND OR')
    not_op = Keyword('NOT')

    expr = Forward()
    atom = (
        (real | integer | string | identifier | Group('(' + expr + ')'))
    )

    # Define expressions
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


def extract_columns_from_check(check):
    """
    Extract column names from a CHECK constraint expression.

    Args:
        check (str): CHECK constraint expression.

    Returns:
        list: List of column names.
    """
    # Use pyparsing to parse the expression and extract identifiers
    identifiers = []

    def identifier_action(token):
        identifiers.append(token[0])

    integer = Word(nums)
    real = Word(nums + ".")
    string = QuotedString("'", escChar='\\')
    identifier = Word(alphas, alphanums + "_$").setName("identifier").addParseAction(identifier_action)

    # Define operators
    arith_op = oneOf('+ - * /')
    comp_op = oneOf('= != <> < > <= >= IN NOT IN LIKE NOT LIKE IS IS NOT')
    bool_op = oneOf('AND OR')
    not_op = Keyword('NOT')

    expr = Forward()
    atom = (
        (real | integer | string | identifier | Group('(' + expr + ')'))
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
    except Exception as e:
        pass  # Ignore parsing errors for now

    # Remove duplicates and SQL keywords/operators
    keywords = {'AND', 'OR', 'NOT', 'IN', 'LIKE', 'IS', 'NULL', 'BETWEEN',
                'EXISTS', 'ALL', 'ANY', 'SOME', 'TRUE', 'FALSE', 'CURRENT_DATE'}
    operators = {'=', '!=', '<>', '<', '>', '<=', '>=', '+', '-', '*', '/', '%', 'IS', 'NOT'}
    columns = [token for token in set(identifiers) if token.upper() not in keywords and token not in operators]
    return columns
