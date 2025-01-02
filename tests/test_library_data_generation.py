import os
import re
import pytest
from datetime import date

from parsing import parse_create_tables
from filling import DataGenerator


@pytest.fixture
def library_sql_script_path():
    return os.path.join("tests", "DB_infos/library_sql_script.sql")


@pytest.fixture
def library_sql_script(library_sql_script_path):
    with open(library_sql_script_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def library_tables_parsed(library_sql_script):
    return parse_create_tables(library_sql_script)


@pytest.fixture
def library_data_generator(library_tables_parsed):
    """
    Config for Library schema, with custom mappings for
    Authors, Books, Members, etc.
    """
    predefined_values = {
        'global': {
            'sex': ['M', 'F'],
        },
        'Categories': {
            'category_name': [
                'Fiction', 'Non-fiction', 'Science', 'History',
                'Biography', 'Fantasy', 'Mystery', 'Romance',
                'Horror', 'Poetry'
            ]
        },
    }
    column_type_mappings = {
        'Authors': {
            'sex': lambda fake, row: fake.random_element(elements=('M','F')),
            'first_name': lambda fake, row: fake.first_name(),
            'last_name': lambda fake, row: fake.last_name(),
            'birth_date': lambda fake, row: fake.date_of_birth(minimum_age=25, maximum_age=90),
        },
        'Books': {
            'isbn': lambda fake, row: ''.join(str(fake.random_digit()) for _ in range(13)),
            'publication_year': lambda fake, row: fake.random_int(min=1900, max=date.today().year),
            'penalty_rate': lambda fake, row: float(fake.random_int(min=1, max=30)),
        },
        'Members': {
            'email': 'email',
            'registration_date': lambda fake, row: fake.date_between(start_date='-5y', end_date='today'),
        },
        'Loans': {
            'loan_date': lambda fake, row: fake.date_between(start_date='-2y', end_date='today'),
            'due_date': lambda fake, row: fake.date_between(start_date='-2y', end_date='today'),
        },
        'Penalties': {
            'penalty_amount': lambda fake, row: float(fake.random_int(min=1, max=100)),
            'penalty_date': lambda fake, row: fake.date_between(start_date='-2y', end_date='today'),
        }
    }
    num_rows_per_table = {
        'Authors': 10,
        'Categories': 5,
        'Books': 30,
        'Members': 10,
        'Loans': 15,
        'Penalties': 5,
    }

    return DataGenerator(
        tables=library_tables_parsed,
        num_rows=10,
        predefined_values=predefined_values,
        column_type_mappings=column_type_mappings,
        num_rows_per_table=num_rows_per_table
    )


def test_parse_create_tables_library(library_tables_parsed):
    """Ensure the Library schema is parsed into table definitions."""
    assert len(library_tables_parsed) > 0, "No tables parsed from library_sql_script.sql"
    expected_tables = {"Authors", "Members", "Books", "Categories", "Loans", "Penalties"}
    assert expected_tables.issubset(library_tables_parsed.keys()), (
        f"Missing some expected tables. Found: {library_tables_parsed.keys()}"
    )


def test_generate_data_library(library_data_generator):
    """Check that data is generated for each Library table."""
    fake_data = library_data_generator.generate_data()
    for table_name in library_data_generator.tables.keys():
        assert table_name in fake_data, f"Missing data for table {table_name}"
        assert len(fake_data[table_name]) > 0, f"No rows generated for table {table_name}"


def test_export_sql_library(library_data_generator):
    """Simple check for INSERT statements and known table names."""
    library_data_generator.generate_data()
    sql_output = library_data_generator.export_as_sql_insert_query()
    assert "INSERT INTO" in sql_output, "No INSERT statements found in SQL"
    assert "Authors" in sql_output, "Expected table 'Authors' not found in SQL"


def test_constraints_library(library_data_generator):
    """
    Basic checks on constraints: ISBN format, email format, date relationships, etc.
    Plus foreign key references:
      - Books -> Authors, Categories
      - Loans -> Books, Members
      - Penalties -> Loans
    """
    data = library_data_generator.generate_data()

    # Gather primary keys for cross-checking
    author_ids = {a["author_id"] for a in data.get("Authors", [])}
    category_ids = {c["category_id"] for c in data.get("Categories", [])}
    book_ids = set()
    member_ids = {m["member_id"] for m in data.get("Members", [])}
    loan_ids = set()

    # 1) Authors: sex in [M, F], etc.
    for author in data.get("Authors", []):
        assert author["sex"] in ("M","F"), f"Invalid sex {author['sex']}"
        assert author["first_name"], "Author first_name is blank"
        assert author["last_name"], "Author last_name is blank"
        assert author["birth_date"], "birth_date is missing"

    # 2) Books: check ISBN, publication_year, penalty_rate, references Authors & Categories
    for book in data.get("Books", []):
        book_ids.add(book["book_id"])

        isbn = book["isbn"]
        assert len(isbn) == 13 and isbn.isdigit(), f"Invalid ISBN: {isbn}"
        year = book["publication_year"]
        assert 1900 <= year <= date.today().year, f"Invalid publication_year {year}"
        assert book["penalty_rate"] > 0, f"penalty_rate must be positive: {book['penalty_rate']}"

        # check FKs
        assert book["author_id"] in author_ids, f"Invalid author_id {book['author_id']} in Books"
        assert book["category_id"] in category_ids, f"Invalid category_id {book['category_id']} in Books"

    # 3) Members: email format, registration_date present
    for mem in data.get("Members", []):
        email = mem["email"]
        assert re.match(r'^[\w\.-]+@[\w\.-]+\.\w{2,}$', email), f"Invalid email format: {email}"
        assert mem["registration_date"], "registration_date is missing"

    # 4) Loans: date relationships + references Books & Members
    for loan in data.get("Loans", []):
        loan_ids.add(loan["loan_id"])

        loan_date = loan["loan_date"]
        due_date = loan["due_date"]
        return_date = loan.get("return_date")

        assert due_date > loan_date, f"due_date {due_date} not > loan_date {loan_date}"
        if return_date:
            assert return_date > loan_date, f"return_date {return_date} not > loan_date {loan_date}"

        # Check FKs
        assert loan["book_id"] in book_ids, f"Invalid book_id {loan['book_id']} in Loans"
        assert loan["member_id"] in member_ids, f"Invalid member_id {loan['member_id']} in Loans"

    # 5) Penalties: penalty_amount > 0, references valid Loans
    for penalty in data.get("Penalties", []):
        amount = penalty["penalty_amount"]
        assert amount > 0, f"Penalty amount must be > 0, got {amount}"
        assert penalty["loan_id"] in loan_ids, f"Invalid loan_id {penalty['loan_id']} in Penalties"