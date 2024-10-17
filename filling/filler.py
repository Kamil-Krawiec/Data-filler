import random
from faker import Faker

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
    unique_combinations = {table: set() for table in tables}

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
        primary_keys[table] = {}
        pk_columns = tables[table].get('primary_key', [])
        for pk in pk_columns:
            primary_keys[table][pk] = 1  # Initialize primary key counters

    # Step 2: Generate primary key values for each table
    for table in table_order:
        pk_columns = tables[table].get('primary_key', [])
        for _ in range(num_rows):
            row = {}
            if len(pk_columns) == 1:
                pk = pk_columns[0]
                pk_info = get_column_info(table, pk)
                if 'SERIAL' in pk_info['type'] or 'INT' in pk_info['type']:
                    row[pk] = primary_keys[table][pk]
                    primary_keys[table][pk] += 1
                else:
                    row[pk] = fake.unique.word()
            else:
                # Handle composite primary keys
                for pk in pk_columns:
                    pk_info = get_column_info(table, pk)
                    if 'SERIAL' in pk_info['type'] or 'INT' in pk_info['type']:
                        row[pk] = primary_keys[table][pk]
                        primary_keys[table][pk] += 1
                    else:
                        row[pk] = fake.unique.word()
            generated_data[table].append(row)

    # Step 3: Assign foreign keys
    for table in table_order:
        fks = tables[table].get('foreign_keys', [])
        for row in generated_data[table]:
            for fk in fks:
                fk_columns = fk['columns']
                ref_table = fk['ref_table']
                ref_columns = fk['ref_columns']
                if ref_table in generated_data and ref_columns:
                    if len(fk_columns) == 1 and len(ref_columns) == 1:
                        ref_col = ref_columns[0]
                        ref_values = [record[ref_col] for record in generated_data[ref_table] if record.get(ref_col) is not None]
                        if ref_values:
                            row[fk_columns[0]] = random.choice(ref_values)
                        else:
                            row[fk_columns[0]] = None
                    else:
                        # Handle composite foreign keys
                        ref_records = generated_data[ref_table]
                        if ref_records:
                            ref_record = random.choice(ref_records)
                            for idx, fk_col in enumerate(fk_columns):
                                ref_col = ref_columns[idx]
                                row[fk_col] = ref_record.get(ref_col)
                        else:
                            for fk_col in fk_columns:
                                row[fk_col] = None
                else:
                    for fk_col in fk_columns:
                        row[fk_col] = None

    # Step 4: Set predefined values and fill other columns
    for table in table_order:
        for row in generated_data[table]:
            sex_value = row.get('sex') if table == 'Authors' else None
            for column in tables[table]['columns']:
                col_name = column['name']
                if col_name in row:
                    continue  # Value already set (primary key or foreign key)

                col_type = column['type']
                constraints = column.get('constraints', [])

                # Set predefined values
                if table in predefined_values and col_name in predefined_values[table]:
                    if col_name == 'sex':
                        row[col_name] = random.choice(predefined_values[table][col_name])
                        sex_value = row[col_name]
                    else:
                        row[col_name] = random.choice(predefined_values[table][col_name])
                    continue

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
                if 'NOT NULL' in constraints:
                    row[col_name] = value
                else:
                    row[col_name] = value if random.choice([True, False]) else None

            # Handle unique constraints
            for unique in tables[table].get('unique_constraints', []):
                unique_val = tuple(row[col] for col in unique)
                while unique_val in unique_combinations[table]:
                    for col in unique:
                        col_info = get_column_info(table, col)
                        if col_info:
                            if 'VARCHAR' in col_info['type']:
                                length = int(col_info['type'].split('(')[1].rstrip(')')) if '(' in col_info['type'] else 20
                                if col.lower() == 'email':
                                    first = fake.first_name()
                                    last = fake.last_name()
                                    domain = random.choice(predefined_values['Members']['email_domain'])
                                    email = f"{first.lower()}.{last.lower()}@{domain}"
                                    row[col] = email[:length]
                                else:
                                    row[col] = fake.unique.word()[:length]
                            elif 'INT' in col_info['type']:
                                row[col] = random.randint(1001, 2000)
                            else:
                                row[col] = fake.word()
                    unique_val = tuple(row[col] for col in unique)
                unique_combinations[table].add(unique_val)

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