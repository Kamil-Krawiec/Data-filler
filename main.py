from parsing.parsing import parse_create_tables
from filling.filler import *
import pprint


sql_script = open("DB_infos/sql_script.sql", "r").read()
tables_parsed = parse_create_tables(sql_script)

fake_data = generate_fake_data(tables_parsed)
pp = pprint.PrettyPrinter(indent=4)
pp.pprint(fake_data)