from parsing.parsing import parse_create_tables
import pprint


sql_script = open("DB_infos/sql_script.sql", "r").read()
tables = parse_create_tables(sql_script)
pp = pprint.PrettyPrinter(indent=4)
pp.pprint(tables)