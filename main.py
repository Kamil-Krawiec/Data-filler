from parsing.parsing import parse_create_tables
from filling.data_generator import DataGenerator  # Adjusted import
import pprint

# Read and parse the SQL script
sql_script = open("DB_infos/sql_script.sql", "r").read()
tables_parsed = parse_create_tables(sql_script)

# Create an instance of DataGenerator with the parsed tables and desired number of rows
data_generator = DataGenerator(tables_parsed, num_rows=100)

# Generate the fake data
fake_data = data_generator.generate_data()

# Pretty-print the generated data
pp = pprint.PrettyPrinter(indent=4)
pp.pprint(fake_data)

# Write sql querry to file
with open("DB_infos/fake_data.sql", "w") as f:
    f.write(data_generator.export_as_sql_insert_query())
