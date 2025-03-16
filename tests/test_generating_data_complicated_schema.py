import os
import re
import pytest
import time
import logging
from datetime import date

from parsing import parse_create_tables
from filling import DataGenerator

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@pytest.fixture
def complicated_schema_sql():
    """
    A more involved schema with multiple tables, composite primary keys,
    foreign keys, and various CHECK constraints (PostgreSQL dialect).
    """
    sql = """
    CREATE TABLE Publishers (
        publisher_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        country VARCHAR(50) NOT NULL
    );

    CREATE TABLE Series (
        series_id SERIAL PRIMARY KEY,
        publisher_id INT NOT NULL,
        series_name VARCHAR(100) NOT NULL,
        start_year INT NOT NULL,
        end_year INT,
        FOREIGN KEY (publisher_id) REFERENCES Publishers(publisher_id),
        CHECK (start_year >= 1900 AND start_year <= end_year)
    );

    CREATE TABLE Volumes (
        volume_num INT NOT NULL,
        series_id INT NOT NULL,
        volume_title VARCHAR(200) NOT NULL,
        issue_date DATE NOT NULL,
        PRIMARY KEY (volume_num, series_id),
        FOREIGN KEY (series_id) REFERENCES Series(series_id)
    );

    CREATE TABLE Orders (
        order_id SERIAL PRIMARY KEY,
        volume_num INT NOT NULL,
        series_id INT NOT NULL,
        order_quantity INT NOT NULL,
        order_date DATE NOT NULL,
        FOREIGN KEY (volume_num, series_id) REFERENCES Volumes(volume_num, series_id),
        CHECK (order_quantity > 0)
    );
    """
    return sql


@pytest.fixture
def complicated_schema_tables(complicated_schema_sql):
    """
    Parse the complicated schema using the 'postgres' dialect.
    """
    return parse_create_tables(complicated_schema_sql, dialect='postgres')


@pytest.fixture
def complicated_generator_params(complicated_schema_tables):
    """
    Define generator parameters for the complicated schema.
    """
    num_rows_per_table = {
        "Publishers": 3000,
        "Series": 4000,
        "Volumes": 6000,
        "Orders": 10000
    }
    predefined_values = {}
    column_type_mappings = {
        "Publishers": {
            "name": lambda fake, row: fake.company(),
            "country": lambda fake, row: fake.country()
        },
        "Series": {
            "series_name": lambda fake, row: f"{fake.word().capitalize()} Collection",
            "start_year": lambda fake, row: fake.random_int(min=1900, max=2000),
            "end_year": lambda fake, row: fake.random_int(min=2001, max=date.today().year)
        },
        "Volumes": {
            "volume_title": lambda fake, row: fake.sentence(nb_words=5),
            "issue_date": lambda fake, row: fake.date_between(start_date="-30y", end_date="today")
        },
        "Orders": {
            "order_quantity": lambda fake, row: fake.random_int(min=1, max=5000),
            "order_date": lambda fake, row: fake.date_between(start_date="-2y", end_date="today")
        }
    }
    return {
        "tables": complicated_schema_tables,
        "num_rows": 3000,
        "predefined_values": predefined_values,
        "column_type_mappings": column_type_mappings,
        "num_rows_per_table": num_rows_per_table
    }


def log_complicated_summary(data, generation_time, mode):
    """
    Log a summary table of generated row counts per table and generation time.
    """
    logger.info("\n===== COMPLICATED SCHEMA DATA GENERATION (%s) RESULTS =====", mode)
    logger.info("{:<15} | {:>10}".format("Table", "Row Count"))
    logger.info("-" * 30)
    for table, rows in data.items():
        logger.info("{:<15} | {:>10}".format(table, len(rows)))
    logger.info("{:<15} | {:>10.2f}".format("Time (s)", generation_time))
    logger.info("-" * 30)


def test_complicated_data_generation_with_repair(complicated_generator_params):
    """
    Standalone test for complicated schema data generation with repair enabled.
    Verifies that no constraint violations remain and logs a summary table.
    """
    data_generator_with = DataGenerator(**complicated_generator_params)
    start = time.time()
    data_with_repair = data_generator_with.generate_data(run_repair=True, print_stats=False)
    generation_time = time.time() - start

    total_violations = 0
    for table, rows in data_with_repair.items():
        for row in rows:
            valid, msg = data_generator_with.is_row_valid(table, row)
            if not valid:
                total_violations += 1
                logger.info("Repaired row in table '%s' violates constraint: %s", table, msg)

    log_complicated_summary(data_with_repair, generation_time, mode="WITH REPAIR")
    assert total_violations == 0, "Constraint violations found in repaired data!"


def test_complicated_data_generation_without_repair(complicated_generator_params):
    """
    Standalone test for complicated schema data generation without repair.
    Logs a summary table along with total constraint violations.
    """
    data_generator_without = DataGenerator(**complicated_generator_params)
    start = time.time()
    data_without_repair = data_generator_without.generate_data(run_repair=False, print_stats=False)
    generation_time = time.time() - start

    violation_count = 0
    for table, rows in data_without_repair.items():
        for row in rows:
            valid, msg = data_generator_without.is_row_valid(table, row)
            if not valid:
                violation_count += 1
                logger.info("Unrepaired row in table '%s' violates constraint: %s", table, msg)

    log_complicated_summary(data_without_repair, generation_time, mode="WITHOUT REPAIR")
    logger.info("Total constraint violations in unrepaired data: %d", violation_count)