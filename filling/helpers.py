import random
import re

import exrex
from pyparsing import ParserElement

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

