import pytest
import time
import logging
from datetime import date

from parsing import parse_create_tables
from filling import DataGenerator


@pytest.fixture
def complicated_schema_sql():
    """
    A more involved schema with multiple tables, composite primary keys,
    foreign keys, and various CHECK constraints.
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
    # Use 'postgres' dialect to parse 'SERIAL'
    return parse_create_tables(complicated_schema_sql, dialect='postgres')


@pytest.fixture
def complicated_data_generator(complicated_schema_tables):
    """
    Set up a DataGenerator targeting around 10,000 rows for the largest table.
    """
    num_rows_per_table = {
        "Publishers": 3000,
        "Series": 4000,
        "Volumes": 6000,
        "Orders": 10000
    }

    column_type_mappings = {
        "Publishers": {
            "name": lambda fake, row: fake.company(),
            "country": "country"
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

    return DataGenerator(
        tables=complicated_schema_tables,
        num_rows=3000,  # baseline if not overridden
        predefined_values={},
        column_type_mappings=column_type_mappings,
        num_rows_per_table=num_rows_per_table
    )


def verify_complicated_schema_data(data, logger):
    """
    Manually verify constraints for the 'complicated' schema.
    data: dict of {table_name: [row_dict, ...]}

    Returns a dictionary with counts of each violation type and total violations.
    """

    # Extract references to check foreign keys. For simplicity, store them in sets/dicts.
    # 1) Publishers PK: publisher_id
    publisher_ids = {row["publisher_id"] for row in data.get("Publishers", [])}

    # 2) Series PK: series_id, references Publishers(publisher_id)
    series_ids = set()
    # 3) Volumes PK: (volume_num, series_id)
    volumes_pk = set()
    # 4) Orders PK: order_id

    # We will also track partial data to check uniqueness in PK or composite keys
    order_ids = set()

    # We'll keep counters for each type of violation
    violations = {
        "not_null": 0,
        "check_constraint": 0,
        "fk_violation": 0,
        "pk_violation": 0
    }

    # ─────────────────────────────────────────────────────────
    # Check Publishers
    # ─────────────────────────────────────────────────────────
    for row in data.get("Publishers", []):
        # Not null checks
        if row["name"] is None or row["country"] is None:
            violations["not_null"] += 1

    # ─────────────────────────────────────────────────────────
    # Check Series
    # ─────────────────────────────────────────────────────────
    for row in data.get("Series", []):
        # PK gather: series_id
        series_id = row["series_id"]
        if series_id in series_ids:
            # Duplicate PK
            violations["pk_violation"] += 1
        else:
            series_ids.add(series_id)

        # Not null checks
        if row["publisher_id"] is None or row["series_name"] is None or row["start_year"] is None:
            violations["not_null"] += 1

        # Foreign key check
        if row["publisher_id"] not in publisher_ids:
            violations["fk_violation"] += 1

        # Check constraint: start_year >= 1900 AND start_year <= end_year
        start_year = row["start_year"]
        end_year = row["end_year"]
        if start_year < 1900 or (end_year is not None and start_year > end_year):
            violations["check_constraint"] += 1

    # ─────────────────────────────────────────────────────────
    # Check Volumes
    # ─────────────────────────────────────────────────────────
    for row in data.get("Volumes", []):
        # Composite PK: (volume_num, series_id)
        pk_tuple = (row["volume_num"], row["series_id"])
        if pk_tuple in volumes_pk:
            violations["pk_violation"] += 1
        else:
            volumes_pk.add(pk_tuple)

        # Not null checks
        if row["volume_num"] is None or row["series_id"] is None or row["volume_title"] is None or row["issue_date"] is None:
            violations["not_null"] += 1

        # Foreign key check => references Series(series_id)
        if row["series_id"] not in series_ids:
            violations["fk_violation"] += 1

    # ─────────────────────────────────────────────────────────
    # Check Orders
    # ─────────────────────────────────────────────────────────
    for row in data.get("Orders", []):
        # PK: order_id
        if row["order_id"] in order_ids:
            violations["pk_violation"] += 1
        else:
            order_ids.add(row["order_id"])

        # Not null
        if row["volume_num"] is None or row["series_id"] is None or row["order_quantity"] is None or row["order_date"] is None:
            violations["not_null"] += 1

        # Foreign key => (volume_num, series_id) in Volumes
        pk_tuple = (row["volume_num"], row["series_id"])
        if pk_tuple not in volumes_pk:
            violations["fk_violation"] += 1

        # Check constraint => order_quantity > 0
        if row["order_quantity"] <= 0:
            violations["check_constraint"] += 1

    total_violations = sum(violations.values())
    return {
        "not_null": violations["not_null"],
        "check_constraint": violations["check_constraint"],
        "fk_violation": violations["fk_violation"],
        "pk_violation": violations["pk_violation"],
        "total": total_violations
    }


def test_complicated_data_generation_with_and_without_repair(complicated_data_generator):
    """
    Generate data for the 'complicated' schema, measure generation time,
    manually verify constraints, and log results in a table.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # ─────────────────────────────────────────────────────────
    # Generate WITH repair
    # ─────────────────────────────────────────────────────────
    start_repair = time.time()
    data_with_repair = complicated_data_generator.generate_data(run_repair=True, print_stats=False)
    time_repair = time.time() - start_repair

    repair_violations = verify_complicated_schema_data(data_with_repair, logger)

    # ─────────────────────────────────────────────────────────
    # Generate WITHOUT repair
    # ─────────────────────────────────────────────────────────
    start_no_repair = time.time()
    data_no_repair = complicated_data_generator.generate_data(run_repair=False, print_stats=False)
    time_no_repair = time.time() - start_no_repair

    norepair_violations = verify_complicated_schema_data(data_no_repair, logger)

    # ─────────────────────────────────────────────────────────
    # Print Summary in Table
    # ─────────────────────────────────────────────────────────
    print("\n===== COMPLICATED SCHEMA TEST RESULTS =====")
    print("{:<30} | {:>12} | {:>12}".format("Metric", "With Repair", "No Repair"))
    print("-" * 60)
    print("{:<30} | {:>12.2f} | {:>12.2f}".format("Generation Time (s)", time_repair, time_no_repair))
    print("{:<30} | {:>12} | {:>12}".format("Not Null Violations", repair_violations["not_null"], norepair_violations["not_null"]))
    print("{:<30} | {:>12} | {:>12}".format("Check Violations", repair_violations["check_constraint"], norepair_violations["check_constraint"]))
    print("{:<30} | {:>12} | {:>12}".format("FK Violations", repair_violations["fk_violation"], norepair_violations["fk_violation"]))
    print("{:<30} | {:>12} | {:>12}".format("PK/Unique Violations", repair_violations["pk_violation"], norepair_violations["pk_violation"]))
    print("{:<30} | {:>12} | {:>12}".format("TOTAL Violations", repair_violations["total"], norepair_violations["total"]))
    print("-" * 60)

    # Assert that repaired data does not have any constraint violations
    assert repair_violations["total"] == 0, (
        f"Found {repair_violations['total']} total violations even after repair!"
    )