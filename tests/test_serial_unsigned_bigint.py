import pytest
import re
from parsing import parse_create_tables
from filling import DataGenerator


@pytest.fixture
def seats_sql():
    """
    This fixture provides the DDL for the `Seats` table, including:
        - `id1` (SERIAL, NOT NULL) but not primary key
        - `id2` (VARCHAR(3) PRIMARY KEY)
        - `row` (BIGINT NOT NULL)
        - `seat` (INT UNSIGNED NOT NULL)
        - `theater_id` (BIGINT UNSIGNED NOT NULL)
    """
    return """
    CREATE TABLE Seats (
        id1 SERIAL NOT NULL,
        id2 VARCHAR(3) NOT NULL PRIMARY KEY,
        row BIGINT NOT NULL,
        seat INT UNSIGNED NOT NULL,
        theater_id BIGINT UNSIGNED NOT NULL
    );
    """


@pytest.fixture
def seats_table_parsed(seats_sql):
    """
    Parse the above SQL and return the dictionary describing the table schema.
    """
    # Dialect could be 'postgres' or 'mysql' based on your environment.
    return parse_create_tables(seats_sql, dialect="postgres")


@pytest.fixture
def seats_data_generator(seats_table_parsed):
    """
    Create a DataGenerator configured to handle the 'Seats' table.
    """
    # We define custom mappings so that:
    #   - `id2` is a short string (3 chars).
    #   - `row` is a random BIGINT (1..10000).
    #   - `seat` is an UNSIGNED INT => we keep it >= 0.
    #   - `theater_id` is an UNSIGNED BIGINT => keep >= 0.
    #   - `id1` is_serial => automatically incremented if your DataGenerator
    #        uses the is_serial flag to do so.

    column_type_mappings = {
        "Seats": {
            "row": lambda fake, row: fake.random_int(min=1, max=9999),
            "seat": lambda fake, row: fake.random_int(min=0, max=500),  # Unsigned
            "theater_id": lambda fake, row: fake.random_int(min=0, max=1000),
            # For `id2`, a short 3-char string
            "id2": lambda fake, row: fake.lexify(text='???')  # e.g., 'abc'
        }
    }

    return DataGenerator(
        tables=seats_table_parsed,
        num_rows=5,  # generate 5 rows for example
        column_type_mappings=column_type_mappings
    )


def test_parse_seats_table(seats_table_parsed):
    """
    Test that the parser correctly identifies the schema for the Seats table:
      - 'id1' is SERIAL (is_serial = True) but not PK
      - 'id2' is PK (VARCHAR(3))
      - 'seat' and 'theater_id' are UNSIGNED
      - primary_key == ['id2']
    """
    assert "Seats" in seats_table_parsed, "Seats table not recognized by parser."
    seats_def = seats_table_parsed["Seats"]

    # Check columns
    columns = {col["name"]: col for col in seats_def["columns"]}
    assert "id1" in columns
    assert "id2" in columns
    assert "row" in columns
    assert "seat" in columns
    assert "theater_id" in columns

    # 1) Verify 'id1' is SERIAL, is_serial = True, but not in primary_key
    id1_col = columns["id1"]
    assert "SERIAL" in id1_col["type"], "Expected 'id1' to be SERIAL."
    assert id1_col.get("is_serial") is True, "Expected 'id1' is_serial to be True."
    # 'id1' constraints might include NOT NULL
    assert "NOT NULL" in id1_col["constraints"], "id1 should be NOT NULL."

    # 2) Verify 'id2' is PK => 'primary_key' list in seats_def
    assert seats_def["primary_key"] == ["id2"], "Expected primary key to be ['id2']."
    id2_col = columns["id2"]
    assert "VARCHAR(3)" in id2_col["type"], "Expected 'id2' to be VARCHAR(3)."
    assert "PRIMARY KEY" in id2_col["constraints"], "Expected 'id2' to have PRIMARY KEY."

    # 3) Verify 'seat' is INT UNSIGNED => UINT
    seat_col = columns["seat"]
    assert "UINT" in seat_col["type"], "seat column should be INT-based."
    assert "NOT NULL" in seat_col["constraints"], "seat should be NOT NULL."

    # 4) Verify 'theater_id' is BIGINT UNSIGNED => UBIGINT
    theater_col = columns["theater_id"]
    assert "UBIGINT" in theater_col["type"], "theater_id should be UNSIGNED."
    assert "NOT NULL" in theater_col["constraints"], "theater_id should be NOT NULL."


def test_generate_data_seats(seats_data_generator):
    """
    After parsing the Seats table, generate data and confirm:
      - 'id1' (SERIAL) is auto-incremented numeric
      - 'id2' (PK, VARCHAR(3)) is a short string
      - 'seat', 'theater_id' are non-negative integers
      - 'row' is a positive integer
    """
    generated_data = seats_data_generator.generate_data()
    seats_rows = generated_data.get("Seats", [])

    assert len(seats_rows) == 5, "Expected 5 rows generated for Seats table."

    # Keep track of 'id1' to confirm it's incrementing
    prev_id1 = 0

    for row in seats_rows:
        # 1) Check 'id1' is numeric and increments
        id1_val = row["id1"]
        # Should be an integer > prev_id1
        assert isinstance(id1_val, int), f"id1 should be an int, got {type(id1_val)}"
        assert id1_val > prev_id1, "id1 not strictly incremented!"
        prev_id1 = id1_val

        # 2) Check 'id2' is a short string (3 chars)
        id2_val = row["id2"]
        assert isinstance(id2_val, str), f"id2 should be a string, got {type(id2_val)}"
        assert len(id2_val) == 3, f"id2 should have length 3, got '{id2_val}'"

        # 3) Check 'row' is a positive integer
        row_val = row["row"]
        assert isinstance(row_val, int), f"'row' should be int, got {type(row_val)}"
        assert row_val > 0, f"'row' should be > 0, got {row_val}"

        # 4) Check 'seat' is non-negative
        seat_val = row["seat"]
        assert isinstance(seat_val, int), f"'seat' should be int, got {type(seat_val)}"
        assert seat_val >= 0, f"'seat' must be >= 0, got {seat_val}"

        # 5) Check 'theater_id' is non-negative
        theater_val = row["theater_id"]
        assert isinstance(theater_val, int), f"'theater_id' should be int, got {type(theater_val)}"
        assert theater_val >= 0, f"'theater_id' must be >= 0, got {theater_val}"
