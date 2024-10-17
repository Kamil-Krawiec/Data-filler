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
    primary_keys = {}
    unique_values = {table: {tuple(u): set() for u in tables[table].get('unique_constraints', [])} for table in tables}

    predefined_values = {
        'Categories': {
            'category_name': ['Fiction', 'Non-fiction', 'Science', 'History', 'Biography', 'Fantasy', 'Mystery']
        },
        'Authors': {
            'sex': ['M', 'F']
        },
        'Members': {
            'email_domain': ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com']
        }
    }

    def get_column_info(table, col_name):
        return next((col for col in tables[table]['columns'] if col['name'] == col_name), None)

    # Determine table generation order based on foreign key dependencies
    table_order = resolve_table_order(tables)

    # Step 1: Generate primary keys
    for table in table_order:
        generated_data[table] = []
        primary_keys[table] = []
        pk_columns = tables[table].get('primary_key', [])
        for i in range(1, num_rows + 1):
            row = {}
            if len(pk_columns) == 1:
                pk = pk_columns[0]
                pk_info = get_column_info(table, pk)
                if 'SERIAL' in pk_info['type']:
                    row[pk] = i
                elif 'INT' in pk_info['type']:
                    row[pk] = i
                else:
                    row[pk] = fake.word()
                primary_keys[table].append(row[pk])
            else:
                # Composite primary keys
                pk_values = []
                for pk in pk_columns:
                    pk_info = get_column_info(table, pk)
                    if 'SERIAL' in pk_info['type']:
                        pk_value = i
                    elif 'INT' in pk_info['type']:
                        pk_value = i
                    else:
                        pk_value = fake.word()
                    row[pk] = pk_value
                    pk_values.append(pk_value)
                primary_keys[table].append(tuple(pk_values))
            generated_data[table].append(row)

    # Step 2: Assign foreign keys
    for table in table_order:
        fks = tables[table].get('foreign_keys', [])
        for row in generated_data[table]:
            for fk in fks:
                fk_columns = fk['columns']
                ref_table = fk['ref_table']
                ref_columns = fk['ref_columns']
                if ref_table in primary_keys and ref_columns:
                    # Assuming single-column foreign keys for simplicity
                    if len(fk_columns) == 1 and len(ref_columns) == 1:
                        ref_column = ref_columns[0]
                        ref_values = primary_keys[ref_table]
                        if len(ref_values) > 0:
                            row[fk_columns[0]] = random.choice(ref_values)
                        else:
                            row[fk_columns[0]] = None
                    else:
                        # Composite foreign keys
                        ref_values = primary_keys[ref_table]
                        if len(ref_values) > 0:
                            ref_value = random.choice(ref_values)
                            for idx, fk_col in enumerate(fk_columns):
                                row[fk_col] = ref_value[idx]
                        else:
                            for fk_col in fk_columns:
                                row[fk_col] = None
                else:
                    for fk_col in fk_columns:
                        row[fk_col] = None

    # Step 3: Fill other columns
    for table in table_order:
        for row in generated_data[table]:
            sex_value = row.get('sex') if table == 'Authors' else None
            for column in tables[table]['columns']:
                col_name = column['name']
                if col_name in tables[table].get('primary_key', []):
                    continue  # Already set
                if any(fk['columns'] == [col_name] for fk in tables[table].get('foreign_keys', [])):
                    continue  # Foreign key already set
                if col_name in predefined_values.get(table, {}):
                    # Predefined value already set
                    continue
                col_type = column['type']
                constraints = column.get('constraints', [])

                # Generate data based on column type
                if 'INT' in col_type:
                    value = random.randint(1, 1000)
                elif 'VARCHAR' in col_type:
                    length = int(col_type.split('(')[1].rstrip(')')) if '(' in col_type else 20
                    if col_name.lower() == 'first_name':
                        if sex_value == 'M':
                            value = fake.first_name_male()[:length]
                        elif sex_value == 'F':
                            value = fake.first_name_female()[:length]
                        else:
                            value = fake.first_name()[:length]
                    elif col_name.lower() == 'last_name':
                        if sex_value == 'M':
                            value = fake.last_name_male()[:length]
                        elif sex_value == 'F':
                            value = fake.last_name_female()[:length]
                        else:
                            value = fake.last_name()[:length]
                    elif col_name.lower() == 'email':
                        first = row.get('first_name', fake.first_name())
                        last = row.get('last_name', fake.last_name())
                        domain = random.choice(predefined_values['Members']['email_domain'])
                        email = f"{first.lower()}.{last.lower()}@{domain}"
                        value = email[:length]
                    elif col_name.lower() == 'category_name':
                        value = random.choice(predefined_values['Categories']['category_name'])[:length]
                    else:
                        value = fake.word()[:length]
                elif 'CHAR' in col_type:
                    length = int(col_type.split('(')[1].rstrip(')')) if '(' in col_type else 1
                    value = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=length))
                elif 'DATE' in col_type:
                    value = fake.date()
                elif 'DECIMAL' in col_type or 'NUMERIC' in col_type:
                    precision, scale = 10, 2
                    if '(' in col_type:
                        parts = col_type.split('(')[1].rstrip(')').split(',')
                        precision = int(parts[0])
                        scale = int(parts[1]) if len(parts) > 1 else 2
                    value = round(random.uniform(0, 1000), scale)
                else:
                    value = fake.word()

                # Handle NOT NULL constraint
                if 'NOT NULL' not in constraints and random.choice([True, False]):
                    row[col_name] = None
                else:
                    row[col_name] = value

            # Handle unique constraints
            for index,unique in enumerate(tables[table].get('unique_constraints', [])):
                unique_val = tuple(row[col] for col in unique)
                existing = [tuple(d[col] for col in unique) for d in generated_data[table]]
                attempts = 0
                while unique_val in existing and attempts < 10:
                    for col in unique:
                        col_info = get_column_info(table, col)
                        if col_info:
                            if 'VARCHAR' in col_info['type']:
                                length = int(col_info['type'].split('(')[1].rstrip(')')) if '(' in col_info['type'] else 20
                                if col.lower() == 'email':
                                    first = row.get('first_name', fake.first_name())
                                    last = row.get('last_name', fake.last_name())
                                    domain = random.choice(predefined_values['Members']['email_domain'])
                                    email = f"{first.lower()}.{last.lower()}@{domain}"
                                    row[col] = email[:length]
                                else:
                                    row[col] = fake.unique.word()[:length]
                            elif 'INT' in col_info['type']:
                                row[col] = random.randint(1, 1000)
                            elif 'SERIAL' in col_info['type']:
                                row[col] = primary_keys[table][col]
                                primary_keys[table][col] += 1
                            else:
                                row[col] = fake.word()
                    unique_val = tuple(row[col] for col in unique)
                    attempts += 1

                unique_values[table][tuple(unique)].add(unique_val)

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
        for fk in details.get('foreign_keys', []):
            if fk.get('ref_table'):
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