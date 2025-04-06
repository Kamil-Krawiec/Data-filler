import pytest
import time
import logging

from parsing import parse_create_tables
from filling import DataGenerator

@pytest.fixture
def simple_schema_sql():
    """
    A very simple schema (just two tables), but we want 1 million rows in the largest table.
    Minimal constraints to avoid heavy overhead.
    """
    sql = """
    CREATE TABLE Users (
        user_id SERIAL PRIMARY KEY,
        username VARCHAR(50) NOT NULL UNIQUE,
        join_year INT NOT NULL
    );

    CREATE TABLE Posts (
        post_id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        content TEXT,
        FOREIGN KEY (user_id) REFERENCES Users(user_id)
    );
    """
    return sql

@pytest.fixture
def simple_schema_tables(simple_schema_sql):
    return parse_create_tables(simple_schema_sql, dialect='postgres')

@pytest.fixture
def simple_data_generator(simple_schema_tables):
    """
    Target ~1,000,000 rows in the largest table to stress test performance.
    """
    num_rows_per_table = {
        "Users": 200000,  # 200k
        "Posts": 1000000  # 1 million
    }

    column_type_mappings = {
    }

    return DataGenerator(
        tables=simple_schema_tables,
        num_rows=100000,  # fallback if not in num_rows_per_table
        predefined_values=None,
        column_type_mappings=column_type_mappings,
        num_rows_per_table=num_rows_per_table
    )


def verify_simple_schema_data(data, logger):
    """
    Manual constraint checks for the 'simple' schema with 2 tables.
    We handle:
      - PK for both tables
      - NOT NULL checks
      - user_id references in Posts
      - username is unique
    Returns a dict with violation counts.
    """

    violations = {
        "not_null": 0,
        "check_constraint": 0,  # we have none explicitly, but let's keep the field
        "fk_violation": 0,
        "unique_violation": 0,
        "pk_violation": 0
    }

    # Gather sets to check foreign keys
    user_ids = set()
    # Also track unique usernames
    usernames = set()

    # Check Users table
    # PK: user_id
    # username: NOT NULL, UNIQUE
    # join_year: NOT NULL
    used_user_ids = set()
    for row in data.get("Users", []):
        uid = row["user_id"]
        # Check PK duplication
        if uid in used_user_ids:
            violations["pk_violation"] += 1
        else:
            used_user_ids.add(uid)
            user_ids.add(uid)

        if row["username"] is None or row["join_year"] is None:
            violations["not_null"] += 1

        # Unique username
        if row["username"] in usernames:
            violations["unique_violation"] += 1
        else:
            usernames.add(row["username"])

    # Check Posts table
    # PK: post_id
    # user_id: not null, must exist in Users
    used_post_ids = set()
    for row in data.get("Posts", []):
        pid = row["post_id"]
        if pid in used_post_ids:
            violations["pk_violation"] += 1
        else:
            used_post_ids.add(pid)

        if row["user_id"] is None:
            violations["not_null"] += 1
        else:
            if row["user_id"] not in user_ids:
                violations["fk_violation"] += 1

        # content can be null by schema definition, so we skip

    total_violations = sum(violations.values())
    return {
        "not_null": violations["not_null"],
        "check_constraint": violations["check_constraint"],
        "fk_violation": violations["fk_violation"],
        "unique_violation": violations["unique_violation"],
        "pk_violation": violations["pk_violation"],
        "total": total_violations
    }


def test_simple_schema_massive_generation(simple_data_generator):
    """
    Generate ~1 million rows in 'Posts'. Then manually check constraints
    and log results in a table. We do this with and without repair.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # 1) With repair
    start_repair = time.time()
    data_repair = simple_data_generator.generate_data()
    time_repair = time.time() - start_repair

    repair_violations = verify_simple_schema_data(data_repair, logger)

    # 2) Without repair
    start_norepair = time.time()
    data_norepair = simple_data_generator.generate_data(run_repair=False, print_stats=False)
    time_norepair = time.time() - start_norepair

    norepair_violations = verify_simple_schema_data(data_norepair, logger)

    # Print final table
    print("\n===== SIMPLE SCHEMA (MASSIVE) TEST RESULTS =====")
    print("{:<35} | {:>12} | {:>12}".format("Metric", "With Repair", "No Repair"))
    print("-" * 66)
    print("{:<35} | {:>12.2f} | {:>12.2f}".format("Generation Time (s)", time_repair, time_norepair))
    print("{:<35} | {:>12} | {:>12}".format("Not Null Violations", repair_violations["not_null"], norepair_violations["not_null"]))
    print("{:<35} | {:>12} | {:>12}".format("Check Violations", repair_violations["check_constraint"], norepair_violations["check_constraint"]))
    print("{:<35} | {:>12} | {:>12}".format("FK Violations", repair_violations["fk_violation"], norepair_violations["fk_violation"]))
    print("{:<35} | {:>12} | {:>12}".format("Unique Violations", repair_violations["unique_violation"], norepair_violations["unique_violation"]))
    print("{:<35} | {:>12} | {:>12}".format("PK Violations", repair_violations["pk_violation"], norepair_violations["pk_violation"]))
    print("{:<35} | {:>12} | {:>12}".format("TOTAL Violations", repair_violations["total"], norepair_violations["total"]))
    print("-" * 66)

    # We expect zero violations after repair
    assert repair_violations["total"] == 0, (
        f"Found {repair_violations['total']} violations even after repair with 1 million rows!"
    )