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
    Advanced checks for the Library schema:

    1) Authors & Categories are each valid on their own
    2) Books references Authors & Categories
    3) Members exist on their own
    4) Loans references Books & Members
    5) Penalties references Loans
    6) Validate typical constraints (ISBN, penalty_rate > 0, etc.)
    """
    data = library_data_generator.generate_data()

    # 1) Check Authors
    author_ids = set()
    for author in data.get("Authors", []):
        aid = author["author_id"]
        author_ids.add(aid)

        # Basic checks
        assert author["sex"] in ("M","F"), f"Invalid sex {author['sex']}"
        assert author["first_name"], "Author first_name is blank"
        assert author["last_name"], "Author last_name is blank"
        assert author["birth_date"], "birth_date is missing"

    # 2) Check Categories
    category_ids = set()
    for cat in data.get("Categories", []):
        cid = cat["category_id"]
        category_ids.add(cid)
        assert cat["category_name"], "category_name is blank"

    # 3) Check Books => references Authors & Categories
    book_ids = set()
    for book in data.get("Books", []):
        bid = book["book_id"]
        book_ids.add(bid)

        isbn = book["isbn"]
        assert len(isbn) == 13 and isbn.isdigit(), f"Invalid ISBN: {isbn}"
        pub_year = book["publication_year"]
        assert 1900 <= pub_year <= date.today().year, f"Invalid publication_year {pub_year}"
        assert book["penalty_rate"] > 0, f"penalty_rate must be positive, got {book['penalty_rate']}"

        # FK checks
        assert book["author_id"] in author_ids, (
            f"Book references nonexistent author_id {book['author_id']}"
        )
        assert book["category_id"] in category_ids, (
            f"Book references nonexistent category_id {book['category_id']}"
        )

    # 4) Check Members
    member_ids = set()
    for mem in data.get("Members", []):
        mid = mem["member_id"]
        member_ids.add(mid)

        email = mem["email"]
        assert re.match(r'^[\w\.-]+@[\w\.-]+\.\w{2,}$', email), f"Invalid email {email}"
        assert mem["registration_date"], "registration_date is missing"

    # 5) Check Loans => references Books & Members
    loan_ids = set()
    for loan in data.get("Loans", []):
        lid = loan["loan_id"]
        loan_ids.add(lid)

        assert loan["book_id"] in book_ids, f"Loan references nonexistent book_id {loan['book_id']}"
        assert loan["member_id"] in member_ids, f"Loan references nonexistent member_id {loan['member_id']}"

        loan_date = loan["loan_date"]
        due_date = loan["due_date"]
        return_date = loan.get("return_date")

        assert due_date > loan_date, (
            f"due_date {due_date} not > loan_date {loan_date}"
        )
        if return_date:
            assert return_date > loan_date, (
                f"return_date {return_date} not > loan_date {loan_date}"
            )

    # 6) Check Penalties => references Loans
    for pen in data.get("Penalties", []):
        assert pen["loan_id"] in loan_ids, f"Penalty references nonexistent loan_id {pen['loan_id']}"
        amount = pen["penalty_amount"]
        assert amount > 0, f"penalty_amount must be > 0, got {amount}"
        assert pen["penalty_date"], "penalty_date is missing"

