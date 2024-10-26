from parsing.parsing import parse_create_tables
from filling.data_generator import DataGenerator
import pprint

# Read and parse the SQL script
sql_script = open("DB_infos/sql_script_library.sql", "r").read()
tables_parsed = parse_create_tables(sql_script)

predefined_values = {
    'Categories': {
        'category_name': ['Fiction', 'Non-fiction', 'Science', 'History', 'Biography', 'Fantasy', 'Mystery', 'Romance',
                          'Horror', 'Poetry']
    },
    'Members': {
        'sex': ['M', 'F'],
    },
    'Authors': {
        'sex': ['M', 'F'],
    }
}
column_type_mappings = {
    'first_name': 'first_name',
    'last_name': 'last_name',
    'email': 'email',
    'birth_date': lambda fake, row: fake.date_of_birth(minimum_age=18, maximum_age=90),
    'registration_date': lambda fake, row: fake.date_between(start_date='-5y', end_date='today')
}

num_rows_per_table = {
    "Categories": 10,
    "Members": 150,
    "Books": 200,
    "Authors": 100,
}

# Create an instance of DataGenerator with the parsed tables and desired number of rows
data_generator = DataGenerator(
    tables_parsed,
    num_rows=10,
    predefined_values=predefined_values,
    column_type_mappings=column_type_mappings,
    num_rows_per_table=num_rows_per_table
)

# Generate the fake data
fake_data = data_generator.generate_data()

# Write sql querry to file
with open("DB_infos/fake_data_library.sql", "w") as f:
    f.write(data_generator.export_as_sql_insert_query())
