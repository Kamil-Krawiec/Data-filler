from parsing.parsing import parse_create_tables
from filling.data_generator import DataGenerator
import pprint
import random

# Read and parse the new SQL script
sql_script = open('DB_infos/ecommerce_sql_script.sql', "r").read()
tables_parsed = parse_create_tables(sql_script)

# Define predefined values for specific columns
predefined_values = {
    'Products': {
        'product_name': [
            'Laptop', 'Smartphone', 'Headphones', 'Camera', 'Tablet',
            'Smartwatch', 'Printer', 'Monitor', 'Keyboard', 'Mouse'
        ]
    },
    'Suppliers': {
        'supplier_name': [
            'TechCorp', 'GadgetSupply', 'ElectroGoods', 'DeviceHub', 'AccessoryWorld'
        ]
    }
}

# Define column type mappings to use specific Faker methods
column_type_mappings = {
    'first_name': 'first_name',
    'last_name': 'last_name',
    'email': 'email',
    'phone': lambda fake, row: fake.phone_number()[:15],
    'registration_date': lambda fake, row: fake.date_between(start_date='-5y', end_date='today'),
    'contact_name': 'name',
    'contact_email': 'email',
    'order_date': lambda fake, row: fake.date_between(start_date='-2y', end_date='today'),
    'supply_price': lambda fake, row: round(random.uniform(10, 1000), 2),
    'price': lambda fake, row: round(random.uniform(20, 2000), 2),
}

# Specify the number of rows to generate for each table
num_rows_per_table = {
    'Customers': 100,
    'Products': 10,
    'Orders': 200,
    'OrderItems': 500,
    'Suppliers': 5,
    'ProductSuppliers': 30,
}

# Create an instance of DataGenerator with the parsed tables and desired number of rows
data_generator = DataGenerator(
    tables=tables_parsed,
    max_attempts=1000,
    num_rows=10,  # Default number of rows for unspecified tables
    predefined_values=predefined_values,
    column_type_mappings=column_type_mappings,
    num_rows_per_table=num_rows_per_table
)

# Generate the fake data
fake_data = data_generator.generate_data()

# Write SQL queries to a file
with open("DB_infos/fake_data_ecommerce.sql", "w") as f:
    f.write(data_generator.export_as_sql_insert_query())