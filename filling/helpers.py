import random
import re

import exrex
from pyparsing import ParserElement

ParserElement.enablePackrat()


def extract_numeric_ranges(constraints: list, col_name: str) -> list:
    """
    Extract numeric ranges from constraints related to a specific column.

    This function parses a list of SQL constraint expressions to identify and extract numeric range conditions
    (e.g., `>=`, `<=`, `>`, `<`, `=`, `BETWEEN`) applied to a specified column. It returns these conditions
    as a list of tuples, where each tuple contains the operator and the corresponding numeric value.

    Args:
        constraints (list): A list of SQL constraint expressions as strings.
        col_name (str): The name of the column from which to extract numeric range constraints.

    Returns:
        list of tuple:
            A list where each tuple consists of an operator (str) and a numeric value (float). For example:
            `[('>=', 1.0), ('<=', 10.0)]`
    """
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


def generate_numeric_value(ranges: list, col_type: str) -> int or float:
    """
    Generate a numeric value based on specified ranges and column type.

    This function takes a list of numeric range conditions and the data type of the column to generate
    a random numeric value that satisfies all specified constraints. It intelligently determines the
    appropriate range for value generation based on the operators provided.

    Args:
        ranges (list of tuple): A list of tuples where each tuple contains an operator (str) and a numeric value (float).
                                 Example: `[('>=', 1.0), ('<=', 10.0)]`
        col_type (str): The SQL data type of the column (e.g., 'INT', 'DECIMAL', 'NUMERIC').

    Returns:
        int or float:
            A randomly generated numeric value within the specified range. The type of the returned value
            matches the column type:
            - Returns an `int` if the column type is integer-based.
            - Returns a `float` if the column type is decimal-based.
    """
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


def generate_value_matching_regex(pattern: str) -> str:
    """
    Generate a value that matches a specified regex pattern.

    This function utilizes the `exrex` library to generate a random string that conforms to the provided
    regular expression pattern. It handles escape sequences and ensures that the generated value is valid
    according to the regex constraints.

    Args:
        pattern (str): The regex pattern that the generated string must match. For example, `'^\d{13}$'` for a 13-digit ISBN.

    Returns:
        str:
            A randomly generated string that matches the given regex pattern. If the pattern is invalid or
            no matching string can be generated, an empty string is returned.
    """
    # Handle escape sequences
    pattern = pattern.encode('utf-8').decode('unicode_escape')
    # Generate a matching string
    try:
        value = exrex.getone(pattern)
        return value
    except Exception as e:
        print(f"Error generating value for pattern '{pattern}': {e}")
        return ''


def extract_regex_pattern(constraints: list, col_name: str) -> list:
    """
    Extract regex patterns from constraints related to a specific column.

    This function scans through a list of SQL constraint expressions to identify any `REGEXP_LIKE` conditions
    applied to a specified column. It extracts and returns the regex patterns used in these constraints.

    Args:
        constraints (list): A list of SQL constraint expressions as strings.
        col_name (str): The name of the column from which to extract regex patterns.

    Returns:
        list of str:
            A list of regex patterns found in the constraints for the specified column. For example:
            `['^\d{13}$', '^[\\w\\.-]+@[\\w\\.-]+\\.\\w{2,}$']`
    """
    patterns = []
    for constraint in constraints:
        matches = re.findall(
            r"REGEXP_LIKE\s*\(\s*{}\s*,\s*'([^']+)'\s*\)".format(col_name),
            constraint, re.IGNORECASE)
        patterns.extend(matches)
    return patterns


def extract_allowed_values(constraints: list, col_name: str) -> list:
    """
    Extract allowed values from constraints related to a specific column.

    This function parses a list of SQL constraint expressions to identify any `IN` clauses that define
    a set of permissible values for a specified column. It extracts and returns these allowed values.

    Args:
        constraints (list): A list of SQL constraint expressions as strings.
        col_name (str): The name of the column from which to extract allowed values.

    Returns:
        list of str:
            A list of allowed values specified in the `IN` clauses for the given column. For example:
            `['Fiction', 'Non-fiction', 'Science']`
    """
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

