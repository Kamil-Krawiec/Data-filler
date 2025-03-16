import os
import re
import time
import pytest
from datetime import date

from parsing import parse_create_tables
from filling import DataGenerator
import logging



# Define a big schema as a SQL script.
@pytest.fixture
def big_schema_sql():
    sql = """
    CREATE TABLE Authors (
        author_id SERIAL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        birth_date DATE NOT NULL
    );

    CREATE TABLE Books (
        book_id SERIAL PRIMARY KEY,
        title VARCHAR(100) NOT NULL,
        publication_year INT NOT NULL,
        author_id INT,
        FOREIGN KEY (author_id) REFERENCES Authors(author_id)
    );

    CREATE TABLE Reviews (
        review_id SERIAL PRIMARY KEY,
        book_id SERIAL,
        rating INT,
        review_text TEXT,
        FOREIGN KEY (book_id) REFERENCES Books(book_id),
        CHECK (rating >= 1 AND rating <= 5)
    );
    """
    return sql


# Parse the schema.
@pytest.fixture
def big_schema_tables(big_schema_sql):
    # Use 'postgres' dialect to support SERIAL types
    return parse_create_tables(big_schema_sql, dialect='postgres')


# Create a DataGenerator configured to produce large amounts of data.
@pytest.fixture
def big_data_generator(big_schema_tables):
    num_rows_per_table = {
        "Authors": 10000,
        "Books": 20000,
        "Reviews": 50000
    }
    predefined_values = {}
    column_type_mappings = {
        "Authors": {
            "first_name": "first_name",
            "last_name": "last_name",
            "birth_date": lambda fake, row: fake.date_of_birth(minimum_age=20, maximum_age=80)
        },
        "Books": {
            "title": lambda fake, row: fake.sentence(nb_words=5),
            "publication_year": lambda fake, row: fake.random_int(min=1900, max=date.today().year)
        },
        "Reviews": {
            "rating": lambda fake, row: fake.random_int(min=1, max=5),
            "review_text": lambda fake, row: fake.text(max_nb_chars=200)
        }
    }
    return DataGenerator(
        tables=big_schema_tables,
        num_rows=10000,
        predefined_values=predefined_values,
        column_type_mappings=column_type_mappings,
        num_rows_per_table=num_rows_per_table
    )



def test_big_data_generation_with_and_without_repair(big_data_generator):
    """
    Generate a large amount of data twice:
      - First, with repair enabled: measure the time and assert that all rows satisfy constraints.
      - Second, without repair: measure the time and count the number of rows that violate constraints.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)


    # Generate data with repair enabled
    start_repair = time.time()
    data_with_repair = big_data_generator.generate_data(run_repair=True, print_stats=False)
    elapsed_repair = time.time() - start_repair
    logger.info("Data generation WITH repair took %.2f seconds", elapsed_repair)

    # Check that all rows in the repaired data satisfy constraints.
    for table, rows in data_with_repair.items():
        for row in rows:
            valid, message = big_data_generator.is_row_valid(table, row)
            assert valid, f"Repaired row in table '{table}' violates constraint: {message}"

    # Generate data without repair enabled.
    start_no_repair = time.time()
    data_without_repair = big_data_generator.generate_data(run_repair=False, print_stats=False)
    elapsed_no_repair = time.time() - start_no_repair
    logger.info("Data generation WITHOUT repair took %.2f seconds", elapsed_no_repair)

    # Count and log the number of violations in the unrepaired data.
    violation_count = 0
    for table, rows in data_without_repair.items():
        for row in rows:
            valid, message = big_data_generator.is_row_valid(table, row)
            if not valid:
                violation_count += 1
                logger.info("Unrepaired row in table '%s' violates constraint: %s", table, message)
    logger.info("Total constraint violations in unrepaired data: %d", violation_count)