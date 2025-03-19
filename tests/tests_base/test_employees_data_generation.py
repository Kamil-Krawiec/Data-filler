import os
import re
import pytest
from datetime import date

from parsing import parse_create_tables
from filling import DataGenerator

@pytest.fixture
def employees_sql_script():
    """
    Returns the CREATE TABLE statements for the employees database schema.
    Typically you'd load from a file, but here we embed the script for clarity.
    """
    return """
CREATE TABLE employees (
    emp_no      INT             NOT NULL,
    birth_date  DATE            NOT NULL,
    first_name  VARCHAR(14)     NOT NULL,
    last_name   VARCHAR(16)     NOT NULL,
    gender      ENUM ('M','F')  NOT NULL,
    hire_date   DATE            NOT NULL,
    PRIMARY KEY (emp_no)
);

CREATE TABLE departments (
    dept_no     CHAR(4)         NOT NULL,
    dept_name   VARCHAR(40)     NOT NULL,
    PRIMARY KEY (dept_no),
    UNIQUE KEY (dept_name)
);

CREATE TABLE dept_emp (
    emp_no      INT         NOT NULL,
    dept_no     CHAR(4)     NOT NULL,
    from_date   DATE        NOT NULL,
    to_date     DATE        NOT NULL,
    KEY         (emp_no),
    KEY         (dept_no),
    FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
    FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE,
    PRIMARY KEY (emp_no, dept_no)
);

CREATE TABLE dept_manager (
    dept_no      CHAR(4)  NOT NULL,
    emp_no       INT      NOT NULL,
    from_date    DATE     NOT NULL,
    to_date      DATE     NOT NULL,
    KEY         (emp_no),
    KEY         (dept_no),
    FOREIGN KEY (emp_no)  REFERENCES employees (emp_no) ON DELETE CASCADE,
    FOREIGN KEY (dept_no) REFERENCES departments (dept_no) ON DELETE CASCADE,
    PRIMARY KEY (emp_no, dept_no)
);

CREATE TABLE titles (
    emp_no      INT          NOT NULL,
    title       VARCHAR(50)  NOT NULL,
    from_date   DATE         NOT NULL,
    to_date     DATE,
    KEY         (emp_no),
    FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
    PRIMARY KEY (emp_no, title, from_date)
);

CREATE TABLE salaries (
    emp_no      INT    NOT NULL,
    salary      INT    NOT NULL,
    from_date   DATE   NOT NULL,
    to_date     DATE   NOT NULL,
    KEY         (emp_no),
    FOREIGN KEY (emp_no) REFERENCES employees (emp_no) ON DELETE CASCADE,
    PRIMARY KEY (emp_no, from_date)
);
"""

@pytest.fixture
def employees_tables_parsed(employees_sql_script):
    """
    Parse the CREATE TABLE statements using your parse_create_tables() function.
    Returns a dict of table definitions.
    """
    return parse_create_tables(employees_sql_script, dialect='mysql')

@pytest.fixture
def employees_data_generator(employees_tables_parsed):
    """
    Create a DataGenerator (or any test-data generator) configured for the employees schema.

    We'll define basic mappings for each table to ensure valid data:
    - employees.emp_no => auto-increment int
    - departments.dept_no => something like 'd001'
    - etc.
    """
    # Example of how you might define custom generation logic
    column_type_mappings = {
        'employees': {
            # For the PK emp_no, the DataGenerator might do auto-increment; or we do it ourselves.
            'emp_no': lambda fake, row: None,  # let auto-increment or is_serial handle it
            'birth_date': lambda fake, row: fake.date_of_birth(minimum_age=18, maximum_age=70),
            'gender': lambda fake, row: fake.random_element(elements=('M','F')),
            'hire_date': lambda fake, row: fake.date_between(start_date='-30y', end_date='today'),
        },
        'departments': {
            'dept_no': lambda fake, row: f"d{fake.random_int(min=1, max=999):03d}",
        },
        'dept_emp': {
            # from_date, to_date => random dates
        },
        'dept_manager': {
            # from_date, to_date => random dates
        },
        'titles': {
            # title => random job title, from_date => random
        },
        'salaries': {
            'salary': lambda fake, row: fake.random_int(min=30000, max=200000),
            # from_date => random
        }
    }

    num_rows_per_table = {
        'employees': 10,
        'departments': 5,
        'dept_emp': 20,
        'dept_manager': 5,
        'titles': 15,
        'salaries': 15,
    }

    # Create the DataGenerator
    return DataGenerator(
        tables=employees_tables_parsed,
        num_rows=5,  # fallback
        column_type_mappings=column_type_mappings,
        num_rows_per_table=num_rows_per_table
    )

def test_parse_employees_schema(employees_tables_parsed):
    """
    Basic check that the employees schema was parsed and recognized.
    """
    assert len(employees_tables_parsed) > 0, "No tables parsed from employees SQL script."
    expected_tables = {"employees", "departments", "dept_emp", "dept_manager", "titles", "salaries"}
    assert expected_tables.issubset(employees_tables_parsed.keys()), (
        f"Missing some expected tables. Found: {employees_tables_parsed.keys()}"
    )

def test_generate_data_employees(employees_data_generator):
    """
    Check that data generation runs without error and returns non-empty results.
    """
    fake_data = employees_data_generator.generate_data()
    for table_name in employees_data_generator.tables.keys():
        assert table_name in fake_data, f"Missing data for table {table_name}"
        assert len(fake_data[table_name]) > 0, f"No rows generated for table {table_name}"

def test_export_sql_employees(employees_data_generator):
    """
    Simple check that we generate SQL containing 'INSERT INTO'
    and a known table name (like 'employees').
    """
    employees_data_generator.generate_data()
    sql_output = employees_data_generator.export_as_sql_insert_query()
    assert "INSERT INTO" in sql_output
    assert "employees" in sql_output

def test_constraints_employees(employees_data_generator):
    """
    Strict checks on:
    - employees PK: emp_no is INT (not null), birth_date, etc.
    - departments PK: dept_no is CHAR(4)
    - foreign keys in dept_emp, dept_manager, titles, salaries
    - composite PK in dept_emp (emp_no, dept_no), etc.
    """
    data = employees_data_generator.generate_data()

    # 1) employees
    employees_ids = set()
    for emp in data["employees"]:
        # Primary key: emp_no (INT)
        eid = emp["emp_no"]
        assert isinstance(eid, int), f"emp_no must be INT, got {type(eid)}"
        employees_ids.add(eid)

        # birth_date, hire_date present?
        assert emp["birth_date"], "birth_date is missing"
        assert emp["hire_date"], "hire_date is missing"
        # gender in [M,F]
        assert emp["gender"] in ("M","F"), f"Invalid gender {emp['gender']}"

    # 2) departments
    dept_ids = set()
    for d in data["departments"]:
        dno = d["dept_no"]
        # dept_no is CHAR(4)? Just ensure it's a string of length 4.
        assert isinstance(dno, str), f"dept_no must be str, got {type(dno)}"
        assert len(dno) == 4, f"dept_no must have length 4, got '{dno}'"
        dept_ids.add(dno)

    # 3) dept_emp => references employees.emp_no and departments.dept_no
    for de in data["dept_emp"]:
        assert de["emp_no"] in employees_ids, f"dept_emp references nonexistent emp_no {de['emp_no']}"
        assert de["dept_no"] in dept_ids, f"dept_emp references nonexistent dept_no {de['dept_no']}"
        # from_date, to_date present?
        assert de["from_date"], "dept_emp from_date missing"
        # to_date can be None or something

    # 4) dept_manager => references employees.emp_no, departments.dept_no
    for dm in data["dept_manager"]:
        assert dm["emp_no"] in employees_ids, f"dept_manager references nonexistent emp_no {dm['emp_no']}"
        assert dm["dept_no"] in dept_ids, f"dept_manager references nonexistent dept_no {dm['dept_no']}"
        assert dm["from_date"], "dept_manager from_date missing"

    # 5) titles => references employees.emp_no
    for t in data["titles"]:
        assert t["emp_no"] in employees_ids, f"titles references nonexistent emp_no {t['emp_no']}"
        # PK (emp_no, title, from_date) => must be unique, but we usually rely on generation logic
        # from_date must exist
        assert t["from_date"], "titles from_date missing"

    # 6) salaries => references employees.emp_no
    for s in data["salaries"]:
        assert s["emp_no"] in employees_ids, f"salaries references nonexistent emp_no {s['emp_no']}"
        # salary is an INT?
        assert isinstance(s["salary"], int), f"salary must be an int, got {type(s['salary'])}"
        assert s["from_date"], "salaries from_date missing"


def test_datatypes_employees(employees_data_generator):
    """
    Validate that the datatypes of generated data match the schema definitions.
    """
    data = employees_data_generator.generate_data()

    # Define expected datatypes for each table
    expected_datatypes = {
        "employees": {
            "emp_no": int,
            "birth_date": date,
            "first_name": str,
            "last_name": str,
            "gender": str,  # 'M' or 'F', but stored as string
            "hire_date": date,
        },
        "departments": {
            "dept_no": str,
            "dept_name": str,
        },
        "dept_emp": {
            "emp_no": int,
            "dept_no": str,
            "from_date": date,
            "to_date": (date, type(None)),  # can be date or None
        },
        "dept_manager": {
            "emp_no": int,
            "dept_no": str,
            "from_date": date,
            "to_date": (date, type(None)),  # can be date or None
        },
        "titles": {
            "emp_no": int,
            "title": str,
            "from_date": date,
            "to_date": (date, type(None)),  # can be date or None
        },
        "salaries": {
            "emp_no": int,
            "salary": int,
            "from_date": date,
            "to_date": date,
        },
    }

    # Iterate over tables and validate each row
    for table_name, rows in data.items():
        assert table_name in expected_datatypes, f"Unexpected table {table_name} found in data"
        table_schema = expected_datatypes[table_name]
        for row in rows:
            for column, expected_type in table_schema.items():
                assert column in row, f"Column {column} missing in table {table_name}"
                value = row[column]
                if isinstance(expected_type, tuple):
                    # Check for multiple valid types (e.g., date or None)
                    assert any(isinstance(value, t) for t in expected_type), (
                        f"Column {column} in table {table_name} has invalid type: {type(value)}. "
                        f"Expected one of {expected_type}"
                    )
                else:
                    assert isinstance(value, expected_type), (
                        f"Column {column} in table {table_name} has invalid type: {type(value)}. "
                        f"Expected {expected_type}"
                    )