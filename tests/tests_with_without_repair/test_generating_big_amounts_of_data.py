import pytest
import time
import logging
from datetime import date

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
    return parse_create_tables(big_schema_sql, dialect='postgres')


@pytest.fixture
def generator_params(big_schema_tables):
    num_rows_per_table = {
        "Authors": 100000,
        "Books": 200000,
        "Reviews": 100000
    }
    predefined_values = {}
    column_type_mappings = {
        "Authors": {
            "first_name": "first_name",
            "last_name": "last_name",
        },
        "Books": {
            "title": lambda fake, row: fake.sentence(nb_words=5),
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


def test_big_data_generation_with_repair(generator_params):
    """
    Generate data for the big schema with repair enabled,
    verify that every row satisfies constraints,
    and log a summary table via the logger.
    """
    data_generator_with = DataGenerator(**generator_params)
    start = time.time()
    data_with_repair = data_generator_with.generate_data(run_repair=True, print_stats=False)
    elapsed = time.time() - start

    # Verify that every row in each table meets the constraints.
    total_violations = 0
    for table, rows in data_with_repair.items():
        for row in rows:
            valid, message = data_generator_with.is_row_valid(table, row)
            if not valid:
                total_violations += 1
                logger.info("Repaired row in table '%s' violates constraint: %s", table, message)

    # Log summary table using the logger.
    logger.info("\n===== BIG SCHEMA WITH REPAIR TEST RESULTS =====")
    logger.info("{:<30} | {:>12}".format("Metric", "With Repair"))
    logger.info("-" * 45)
    logger.info("{:<30} | {:>12.2f}".format("Generation Time (s)", elapsed))
    logger.info("{:<30} | {:>12}".format("Total Constraint Violations", total_violations))
    logger.info("-" * 45)

    # Assert that repaired data has no constraint violations.
    assert total_violations == 0, "Found constraint violations in repaired data!"


def test_big_data_generation_without_repair(generator_params):
    """
    Generate data for the big schema without repair,
    count and log constraint violations,
    and output a summary table via the logger.
    """
    data_generator_without = DataGenerator(**generator_params)
    start = time.time()
    data_without_repair = data_generator_without.generate_data(run_repair=False, print_stats=False)
    elapsed = time.time() - start

    violation_count = 0
    for table, rows in data_without_repair.items():
        for row in rows:
            valid, message = data_generator_without.is_row_valid(table, row)
            if not valid:
                violation_count += 1
                logger.info("Unrepaired row in table '%s' violates constraint: %s", table, message)

    logger.info("\n===== BIG SCHEMA WITHOUT REPAIR TEST RESULTS =====")
    logger.info("{:<30} | {:>12}".format("Metric", "Without Repair"))
    logger.info("-" * 45)
    logger.info("{:<30} | {:>12.2f}".format("Generation Time (s)", elapsed))
    logger.info("{:<30} | {:>12}".format("Total Constraint Violations", violation_count))
    logger.info("-" * 45)