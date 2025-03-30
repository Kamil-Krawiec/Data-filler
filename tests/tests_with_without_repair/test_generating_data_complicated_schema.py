import os
import re
import pytest
import time
import logging
from datetime import date, datetime

from parsing import parse_create_tables
from filling import DataGenerator

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@pytest.fixture
def complicated_schema_sql():
    """
    A more involved schema (PostgreSQL dialect) with multiple tables, composite keys,
    foreign keys, and CHECK constraints.
    """
    sql = """
    CREATE TABLE Publishers (
        publisher_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        country VARCHAR(50) NOT NULL,
        established_year INT CHECK (established_year >= 1500)
    );

    CREATE TABLE Series (
        series_id SERIAL PRIMARY KEY,
        publisher_id INT NOT NULL,
        series_name VARCHAR(100) NOT NULL,
        start_year INT NOT NULL,
        end_year INT,
        FOREIGN KEY (publisher_id) REFERENCES Publishers(publisher_id) ON DELETE CASCADE,
        CHECK (start_year >= 1900 AND (end_year IS NULL OR start_year <= end_year))
    );

    CREATE TABLE Volumes (
        volume_num INT NOT NULL,
        series_id INT NOT NULL,
        volume_title VARCHAR(200) NOT NULL,
        issue_date DATE NOT NULL,
        PRIMARY KEY (volume_num, series_id),
        FOREIGN KEY (series_id) REFERENCES Series(series_id) ON DELETE CASCADE,
        CHECK (issue_date > '1900-01-01')
    );

    CREATE TABLE Orders (
        order_id SERIAL PRIMARY KEY,
        volume_num INT NOT NULL,
        series_id INT NOT NULL,
        order_quantity INT NOT NULL CHECK (order_quantity > 0),
        order_date DATE NOT NULL,
        FOREIGN KEY (volume_num, series_id) REFERENCES Volumes(volume_num, series_id)
    );

    CREATE TABLE Authors (
        author_id SERIAL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        birth_year INT CHECK (birth_year > 1800)
    );

    CREATE TABLE Books (
        book_id SERIAL PRIMARY KEY,
        title VARCHAR(150) NOT NULL,
        publication_year INT CHECK (publication_year BETWEEN 1900 AND EXTRACT(YEAR FROM CURRENT_DATE)),
        series_id INT,
        FOREIGN KEY (series_id) REFERENCES Series(series_id)
    );

    CREATE TABLE BookAuthors (
        book_id INT NOT NULL,
        author_id INT NOT NULL,
        PRIMARY KEY (book_id, author_id),
        FOREIGN KEY (book_id) REFERENCES Books(book_id) ON DELETE CASCADE,
        FOREIGN KEY (author_id) REFERENCES Authors(author_id) ON DELETE CASCADE
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
        "Publishers": 100,
        "Series": 200,
        "Volumes": 300,
        "Orders": 500,
        "Authors": 150,
        "Books": 200,
        "BookAuthors": 300
    }
    predefined_values = {}
    column_type_mappings = {
        "Publishers": {
            "name": lambda fake, row: fake.company(),
            "country": lambda fake, row: fake.country(),
            "established_year": lambda fake, row: fake.random_int(min=1500, max=1950)
        },
        "Series": {
            "series_name": lambda fake, row: f"{fake.word().capitalize()} Collection",
        },
        "Volumes": {
            "volume_title": lambda fake, row: fake.sentence(nb_words=5),
            "issue_date": lambda fake, row: fake.date_between(start_date="-30y", end_date="today")
        },
        "Orders": {
            "order_quantity": lambda fake, row: fake.random_int(min=1, max=5000),
            "order_date": lambda fake, row: fake.date_between(start_date="-2y", end_date="today")
        },
        "Authors": {
            "first_name": lambda fake, row: fake.first_name(),
            "last_name": lambda fake, row: fake.last_name(),
        },
        "Books": {
            "title": lambda fake, row: fake.sentence(nb_words=5),
        },
    }
    return {
        "tables": complicated_schema_tables,
        "num_rows": 50,  # Fallback if not overridden
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


def check_complicated_data_validity(data):
    """
    Standalone function to verify constraints for the complicated schema.
    Returns a dictionary with violation counts.
    """
    violations = {
        "not_null": 0,
        "check_constraint": 0,
        "fk_violation": 0,
        "pk_violation": 0
    }

    # Publishers: Check not null and established_year constraint.
    publishers = data.get("Publishers", [])
    pub_ids = set()
    for row in publishers:
        if row.get("name") is None or row.get("country") is None or row.get("established_year") is None:
            violations["not_null"] += 1
        if row.get("established_year") is not None and row["established_year"] < 1500:
            violations["check_constraint"] += 1
        pub_ids.add(row.get("publisher_id"))

    # Series: Check FK to Publishers and check constraint on years.
    series = data.get("Series", [])
    series_ids = set()
    for row in series:
        if row.get("publisher_id") not in pub_ids:
            violations["fk_violation"] += 1
        if row.get("series_name") is None or row.get("start_year") is None:
            violations["not_null"] += 1
        start_year = row.get("start_year")
        end_year = row.get("end_year")
        if start_year is not None:
            if start_year < 1900:
                violations["check_constraint"] += 1
            if end_year is not None and start_year > end_year:
                violations["check_constraint"] += 1
        series_ids.add(row.get("series_id"))

    # Volumes: Check composite PK uniqueness, FK to Series, and issue_date constraint.
    volumes = data.get("Volumes", [])
    volumes_pk = set()
    for row in volumes:
        pk = (row.get("volume_num"), row.get("series_id"))
        if None in pk:
            violations["not_null"] += 1
        if pk in volumes_pk:
            violations["pk_violation"] += 1
        else:
            volumes_pk.add(pk)
        if row.get("issue_date") is None:
            violations["not_null"] += 1
        else:
            # Check issue_date > '1900-01-01'
            try:
                issue_date = datetime.strptime(str(row["issue_date"]), "%Y-%m-%d").date()
                if issue_date <= date(1900, 1, 1):
                    violations["check_constraint"] += 1
            except Exception:
                violations["check_constraint"] += 1
        if row.get("series_id") not in series_ids:
            violations["fk_violation"] += 1

    # Orders: Check order_quantity constraint and FK to Volumes.
    orders = data.get("Orders", [])
    order_ids = set()
    for row in orders:
        if row.get("order_id") in order_ids:
            violations["pk_violation"] += 1
        else:
            order_ids.add(row.get("order_id"))
        if (row.get("order_quantity") is None or row.get("order_date") is None):
            violations["not_null"] += 1
        if row.get("order_quantity") is not None and row["order_quantity"] <= 0:
            violations["check_constraint"] += 1
        fk = (row.get("volume_num"), row.get("series_id"))
        if fk not in volumes_pk:
            violations["fk_violation"] += 1

    # Authors: Check not null for names and birth_year constraint.
    authors = data.get("Authors", [])
    author_ids = set()
    for row in authors:
        if row.get("first_name") is None or row.get("last_name") is None or row.get("birth_year") is None:
            violations["not_null"] += 1
        if row.get("birth_year") is not None and row["birth_year"] <= 1800:
            violations["check_constraint"] += 1
        author_ids.add(row.get("author_id"))

    # Books: Check not null for title and publication_year constraint, and FK (if series_id is provided).
    books = data.get("Books", [])
    book_ids = set()
    for row in books:
        if row.get("title") is None or row.get("publication_year") is None:
            violations["not_null"] += 1
        pub_year = row.get("publication_year")
        current_year = date.today().year
        if pub_year is not None and (pub_year < 1900 or pub_year > current_year):
            violations["check_constraint"] += 1
        # If series_id is provided, check existence.
        if row.get("series_id") is not None and row.get("series_id") not in series_ids:
            violations["fk_violation"] += 1
        book_ids.add(row.get("book_id"))

    # BookAuthors: Check composite PK uniqueness and FKs to Books and Authors.
    book_authors = data.get("BookAuthors", [])
    ba_pk = set()
    for row in book_authors:
        pk = (row.get("book_id"), row.get("author_id"))
        if None in pk:
            violations["not_null"] += 1
        if pk in ba_pk:
            violations["pk_violation"] += 1
        else:
            ba_pk.add(pk)
        if row.get("book_id") not in book_ids:
            violations["fk_violation"] += 1
        if row.get("author_id") not in author_ids:
            violations["fk_violation"] += 1

    total = sum(violations.values())
    violations["total"] = total
    return violations


def test_complicated_data_generation_with_repair(complicated_generator_params):
    """
    Standalone test for complicated schema data generation with repair enabled.
    Generates data, checks validity using standalone functions, and logs a summary table.
    """
    data_generator_with = DataGenerator(**complicated_generator_params)
    start = time.time()
    data_with_repair = data_generator_with.generate_data(run_repair=True, print_stats=False)
    generation_time = time.time() - start

    validity = check_complicated_data_validity(data_with_repair)
    log_complicated_summary(data_with_repair, generation_time, mode="WITH REPAIR")
    logger.info("Validation violations (WITH REPAIR): %s", validity)
    assert validity["total"] == 0, "Repaired data contains constraint violations!"


def test_complicated_data_generation_without_repair(complicated_generator_params):
    """
    Standalone test for complicated schema data generation without repair.
    Generates data, checks validity using standalone functions, and logs a summary table.
    """
    data_generator_without = DataGenerator(**complicated_generator_params)
    start = time.time()
    data_without_repair = data_generator_without.generate_data(run_repair=False, print_stats=False)
    generation_time = time.time() - start

    validity = check_complicated_data_validity(data_without_repair)
    log_complicated_summary(data_without_repair, generation_time, mode="WITHOUT REPAIR")
    logger.info("Validation violations (WITHOUT REPAIR): %s", validity)