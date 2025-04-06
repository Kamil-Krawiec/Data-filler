import os
import re
import pytest
from datetime import date, datetime

from parsing import parse_create_tables
from filling import DataGenerator


@pytest.fixture
def various_pk_sql():
    """
    A single fixture returning multiple table definitions,
    each with a different primary key style:
      1) A single DATE pk
      2) A composite of (DATE, INT)
      3) A composite of (DATE, VARCHAR)
    Plus the Seats table example, to keep the test wide.
    """

    return """
    -- 1) Single date PK
    CREATE TABLE date_pk_table (
        date_id DATE NOT NULL,
        some_col VARCHAR(50) NOT NULL,
        PRIMARY KEY (date_id)
    );

    -- 2) Composite PK: (date_id, my_int)
    CREATE TABLE date_int_pk_table (
        date_id DATE NOT NULL,
        my_int INT NOT NULL,
        some_col VARCHAR(50),
        PRIMARY KEY (date_id, my_int)
    );

    -- 3) Composite PK: (dt, short_txt)
    CREATE TABLE date_varchar_pk_table (
        dt DATE NOT NULL,
        short_txt VARCHAR(5) NOT NULL,
        another_col INT,
        PRIMARY KEY (dt, short_txt)
    );

    -- Re-include the 'Seats' table for a wide test
    CREATE TABLE Seats (
        id1 SERIAL NOT NULL,
        id2 VARCHAR(3) NOT NULL PRIMARY KEY,
        row BIGINT NOT NULL CHECK ( row > 0 ),
        seat INT UNSIGNED NOT NULL,
        theater_id BIGINT UNSIGNED NOT NULL
    );
    """


@pytest.fixture
def various_pk_tables_parsed(various_pk_sql):
    """
    Parse the multi-table DDL above (with your parse_create_tables).
    """
    # If your parser needs a dialect, e.g., 'postgres' or 'mysql', specify it:
    return parse_create_tables(various_pk_sql, dialect="postgres")


@pytest.fixture
def various_pk_data_generator(various_pk_tables_parsed):
    """
    Create a DataGenerator that handles these four tables.
    We'll define custom column mappings as needed.
    """
    # For seats, we reuse the logic from your seats example:


    return DataGenerator(
        tables=various_pk_tables_parsed,
        num_rows=100,
    )


def test_parse_various_pk_tables(various_pk_tables_parsed):
    """
    Ensure the parser finds all 4 tables: date_pk_table, date_int_pk_table,
    date_varchar_pk_table, and Seats.
    """
    expected = {"date_pk_table", "date_int_pk_table", "date_varchar_pk_table", "Seats"}
    assert expected.issubset(various_pk_tables_parsed.keys()), (
        f"Missing some expected tables. Found: {various_pk_tables_parsed.keys()}"
    )


def test_generate_data_various_pk(various_pk_data_generator):
    """
    Generate data for all 4 tables. Ensure row counts & basic structure.
    """
    data = various_pk_data_generator.generate_data()

    for tbl in ("date_pk_table", "date_int_pk_table", "date_varchar_pk_table", "Seats"):
        assert tbl in data, f"Missing {tbl} from generated data"
        assert len(data[tbl]) == 100, f"Expected 100 rows in {tbl}, found {len(data[tbl])}"


def test_constraints_date_pk_table(various_pk_data_generator):
    """
    Single date PK => ensure 'date_id' is a date and is used as PK.
    """
    data = various_pk_data_generator.generate_data()
    rows = data["date_pk_table"]
    # Check that 'date_id' is indeed a date and we have some_col
    date_ids = set()
    for r in rows:
        assert isinstance(r["date_id"], date), f"date_id must be a date, got {type(r['date_id'])}"
        date_ids.add(r["date_id"])
        assert "some_col" in r

    # Optional check: ensure uniqueness if you expect no duplicates
    assert len(date_ids) == len(rows), "date_id not unique in date_pk_table!"


def test_constraints_date_int_pk_table(various_pk_data_generator):
    """
    Composite PK: (date_id, my_int)
    Confirm both are present & typed, presumably unique combos.
    """
    data = various_pk_data_generator.generate_data()
    rows = data["date_int_pk_table"]

    combos = set()
    for r in rows:
        did = r["date_id"]
        mid = r["my_int"]
        assert isinstance(did, date), f"date_id must be date, got {type(did)}"
        assert isinstance(mid, int), f"my_int must be int, got {type(mid)}"
        combos.add((did, mid))

    # Check we have unique combos
    assert len(combos) == len(rows), "Composite PK (date_id, my_int) not unique!"


def test_constraints_date_varchar_pk_table(various_pk_data_generator):
    """
    Composite PK: (dt, short_txt)
    dt => date, short_txt => 5-char string
    """
    data = various_pk_data_generator.generate_data()
    rows = data["date_varchar_pk_table"]

    combos = set()
    for r in rows:
        dt = r["dt"]
        txt = r["short_txt"]
        assert isinstance(dt, date), f"dt must be a date, got {type(dt)}"
        assert isinstance(txt, str), f"short_txt must be a string, got {type(txt)}"
        # We only know it's 'up to 5 chars', but let's see if the generator uses exactly 5 or fewer
        assert len(txt) <= 5, f"short_txt should be <= 5 chars, got '{txt}'"
        combos.add((dt, txt))

    # Check uniqueness
    assert len(combos) == len(rows), "(dt, short_txt) combos not unique in date_varchar_pk_table!"


def test_constraints_seats(various_pk_data_generator):
    """
    The same Seats test from earlier, verifying that:
      - id1 is auto-increment
      - id2 is 3-char string (PK)
      - seat, theater_id >= 0
      - row > 0
    """
    data = various_pk_data_generator.generate_data()
    seats_rows = data["Seats"]
    assert len(seats_rows) == 100, "Expected 100 rows in Seats table"

    prev_id1 = 0
    for row in seats_rows:
        # id1: auto-increment
        id1_val = row["id1"]
        assert isinstance(id1_val, int), f"id1 must be an int, got {type(id1_val)}"
        assert id1_val > prev_id1, f"id1 not strictly incremented! {id1_val} <= {prev_id1}"
        prev_id1 = id1_val

        # id2: 3-char PK
        id2_val = row["id2"]
        assert isinstance(id2_val, str), f"id2 is not a string"
        assert len(id2_val) == 3, f"id2 must be length 3, got '{id2_val}'"

        # row: positive int
        row_val = row["row"]
        assert isinstance(row_val, int) and row_val > 0, f"row must be >0, got {row_val}"

        # seat: non-negative
        seat_val = row["seat"]
        assert isinstance(seat_val, int) and seat_val >= 0, f"seat must be >=0, got {seat_val}"

        # theater_id: non-negative
        tid = row["theater_id"]
        assert isinstance(tid, int) and tid >= 0, f"theater_id must be >=0, got {tid}"