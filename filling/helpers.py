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


def generate_column_value(column, fake, constraints=None):
    constraints = constraints or []
    col_name = column['name']
    col_type = column['type'].upper()

    # PREDEFINED VALUES:
    if col_name in ['sex']:
        return random.choice(['M', 'F'])
    if col_name in ['name', 'first_name', 'firstname']:
        sex = column.get('sex', None)
        return fake.first_name_male() if sex == 'M' else fake.first_name_female()
    elif col_name in ['surname', 'last_name', 'lastname']:
        sex = column.get('sex', None)
        return fake.last_name_male() if sex == 'M' else fake.last_name_female()
    elif col_name == 'email':
        name = column.get('first_name', fake.word())
        surname = column.get('last_name', fake.word())
        return f"{name}.{surname}@{fake.free_email_domain()}"
    elif col_name == 'phone':
        return fake.phone_number()
    elif col_name == 'address':
        return fake.address()
    elif col_name == 'city':
        return fake.city()
    elif col_name == 'country':
        return fake.country()
    elif col_name == 'company':
        return fake.company()
    elif col_name == 'job_title':
        # Use predefined values or Faker
        return fake.job()

    # Check for regex constraints
    regex_patterns = extract_regex_pattern(constraints, col_name)
    if regex_patterns:
        # For simplicity, use the first pattern
        pattern = regex_patterns[0]
        return generate_value_matching_regex(pattern)

    # Check for allowed values (IN constraints)
    allowed_values = extract_allowed_values(constraints, col_name)
    if allowed_values:
        return select_allowed_value(allowed_values)

    # Check for numeric ranges
    numeric_ranges = extract_numeric_ranges(constraints, col_name)
    if numeric_ranges:
        return generate_numeric_value(numeric_ranges, col_type)

    # Map SQL data types to generic types
    if re.match(r'.*\b(INT|INTEGER|SMALLINT|BIGINT)\b.*', col_type):
        return random.randint(1, 10000)
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
    elif re.match(r'.*\b(DATE)\b.*', col_type):
        return fake.date()
    elif re.match(r'.*\b(TIMESTAMP|DATETIME)\b.*', col_type):
        return fake.date_time()
    elif re.match(r'.*\b(TIME)\b.*', col_type):
        return fake.time()
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
    else:
        # Default to text for unknown types
        return fake.word()
