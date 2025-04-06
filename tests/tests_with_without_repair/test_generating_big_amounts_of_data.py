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
    params = {
        "tables": big_schema_tables,
        "num_rows": 10000,
        "predefined_values": predefined_values,
        "num_rows_per_table": num_rows_per_table
    }
    return params


def test_big_data_generation_without_repair(generator_params):
    """
    Generate data for the big schema without repair,
    count and log constraint violations,
    and output a summary table via the logger.
    """
    data_generator_without = DataGenerator(**generator_params)
    start = time.time()
    data_without_repair = data_generator_without.generate_data()
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