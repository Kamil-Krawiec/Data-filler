import os
import re
import time
import pytest
from datetime import date
import logging

from parsing import parse_create_tables
from filling import DataGenerator

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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


@pytest.fixture
def big_schema_tables(big_schema_sql):
    # Use 'postgres' dialect to support SERIAL types
    return parse_create_tables(big_schema_sql, dialect='postgres')


# Instead of one fixture, we build the generator parameters as a fixture.
@pytest.fixture
def generator_params(big_schema_tables):
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
    params = {
        "tables": big_schema_tables,
        "num_rows": 10000,
        "predefined_values": predefined_values,
        "column_type_mappings": column_type_mappings,
        "num_rows_per_table": num_rows_per_table
    }
    return params


def test_big_data_generation_with_and_without_repair(generator_params):
    # Create two separate DataGenerator instances using the same parameters.
    data_generator_with = DataGenerator(**generator_params)
    data_generator_without = DataGenerator(**generator_params)

    # Generate data with repair enabled.
    start_repair = time.time()
    data_with_repair = data_generator_with.generate_data(run_repair=True, print_stats=False)
    elapsed_repair = time.time() - start_repair
    logger.info("Data generation WITH repair took %.2f seconds", elapsed_repair)

    # Verify that every row in the repaired data satisfies constraints.
    for table, rows in data_with_repair.items():
        for row in rows:
            valid, message = data_generator_with.is_row_valid(table, row)
            assert valid, f"Repaired row in table '{table}' violates constraint: {message}"

    # Generate data without repair enabled.
    start_no_repair = time.time()
    data_without_repair = data_generator_without.generate_data(run_repair=False, print_stats=False)
    elapsed_no_repair = time.time() - start_no_repair
    logger.info("Data generation WITHOUT repair took %.2f seconds", elapsed_no_repair)

    # Count constraint violations in the unrepaired data.
    violation_count = 0
    for table, rows in data_without_repair.items():
        for row in rows:
            valid, message = data_generator_without.is_row_valid(table, row)
            if not valid:
                violation_count += 1
                logger.info("Unrepaired row in table '%s' violates constraint: %s", table, message)
    logger.info("Total constraint violations in unrepaired data: %d", violation_count)