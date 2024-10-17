import random
from faker import Faker
import pprint

fake = Faker()


def generate_fake_data(tables, num_rows=10):
    """
    Generates fake data based on the parsed table schema with predefined values
    for specific columns and ensures consistency between related columns.

    Args:
        tables (dict): Parsed table schema.
        num_rows (int): Number of rows to generate per table.

    Returns:
        dict: A dictionary with table names as keys and lists of fake records as values.
    """
    generated_data = {}
    references = {}

    predefined_values = {
        'Categories': {
            'category_name': ['Fiction', 'Non-fiction', 'Science', 'History', 'Biography', 'Fantasy', 'Mystery']
        },
        'Authors': {
            'sex': ['M', 'F']
        }
    }

    # Determine table generation order based on foreign key dependencies
    table_order = resolve_table_order(tables)

    for table in table_order:
        generated_data[table] = []
        for _ in range(num_rows):
            row = {}
            sex_value = None
            for column in tables[table]['columns']:
                col_name = column['name']
                col_type = column['type']
                constraints = column['constraints']
                fk = column['foreign_key']

                # Handle foreign keys
                if fk:
                    ref_table = fk['ref_table']
                    ref_column = fk['ref_columns'][0] if fk['ref_columns'] else None
                    if ref_table and ref_column and ref_table in generated_data:
                        row[col_name] = random.choice(generated_data[ref_table])[ref_column]
                    else:
                        row[col_name] = None
                    continue

                # Handle predefined values
                if table in predefined_values and col_name in predefined_values[table]:
                    row[col_name] = random.choice(predefined_values[table][col_name])
                    if col_name == 'sex':
                        sex_value = row[col_name]
                    continue

                # Generate data based on column type
                if 'INT' in col_type:
                    row[col_name] = random.randint(1, 1000)
                elif 'SERIAL' in col_type or 'PRIMARY KEY' in constraints:
                    row[col_name] = len(generated_data[table]) + 1
                elif 'VARCHAR' in col_type:
                    length = int(col_type.split('(')[1].rstrip(')')) if '(' in col_type else 20
                    if col_name.lower() == 'first_name':
                        if sex_value == 'M':
                            row[col_name] = fake.first_name_male()[:length]
                        elif sex_value == 'F':
                            row[col_name] = fake.first_name_female()[:length]
                        else:
                            row[col_name] = fake.first_name()[:length]
                    elif col_name.lower() == 'last_name':
                        if sex_value == 'M':
                            row[col_name] = fake.last_name_male()[:length]
                        elif sex_value == 'F':
                            row[col_name] = fake.last_name_female()[:length]
                        else:
                            row[col_name] = fake.last_name()[:length]
                    elif col_name.lower() == 'category_name':
                        row[col_name] = random.choice(predefined_values['Categories']['category_name'])[:length]
                    else:
                        row[col_name] = fake.word()[:length]
                elif 'CHAR' in col_type:
                    length = int(col_type.split('(')[1].rstrip(')')) if '(' in col_type else 1
                    row[col_name] = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=length))
                elif 'DATE' in col_type:
                    row[col_name] = fake.date()
                elif 'DECIMAL' in col_type or 'NUMERIC' in col_type:
                    precision, scale = 10, 2
                    if '(' in col_type:
                        parts = col_type.split('(')[1].rstrip(')').split(',')
                        precision = int(parts[0])
                        scale = int(parts[1]) if len(parts) > 1 else 2
                    row[col_name] = round(random.uniform(0, 1000), scale)
                else:
                    row[col_name] = fake.word()

                # Handle NOT NULL constraint
                if 'NOT NULL' not in constraints and random.choice([True, False]):
                    row[col_name] = None

            # Handle unique constraints
            for unique in tables[table]['unique_constraints']:
                unique_value = tuple(row[col] for col in unique)
                existing = [tuple(d[col] for col in unique) for d in generated_data[table]]
                while unique_value in existing:
                    for col in unique:
                        if 'VARCHAR' in tables[table]['columns'][tables[table]['columns'].index({'name': col})]['type']:
                            row[col] = fake.unique.word()[:int(
                                tables[table]['columns'][tables[table]['columns'].index({'name': col})]['type'].split(
                                    '(')[1].rstrip(')'))]
                        elif 'INT' in tables[table]['columns'][tables[table]['columns'].index({'name': col})]['type']:
                            row[col] = random.randint(1, 1000)
                    unique_value = tuple(row[col] for col in unique)
                    existing = [tuple(d[col] for col in unique) for d in generated_data[table]]

            generated_data[table].append(row)

    return generated_data


def resolve_table_order(tables):
    """
    Resolves the order of table creation based on foreign key dependencies.

    Args:
        tables (dict): Parsed table schema.

    Returns:
        list: Ordered list of table names.
    """
    order = []
    dependencies = {table: set() for table in tables}

    for table, details in tables.items():
        for fk in details['foreign_keys']:
            if fk['ref_table']:
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