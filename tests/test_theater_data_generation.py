import os
import re
import pytest
from parsing import parse_create_tables
from filling import DataGenerator


@pytest.fixture
def theater_sql_script_path():
    return os.path.join("tests", "DB_infos/theater_sql_script.sql")


@pytest.fixture
def theater_sql_script(theater_sql_script_path):
    with open(theater_sql_script_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def theater_tables_parsed(theater_sql_script):
    return parse_create_tables(theater_sql_script)


@pytest.fixture
def theater_data_generator(theater_tables_parsed):
    """
    Returns a DataGenerator instance configured for the Theater schema.
    """
    predefined_values = {}
    column_type_mappings = {
        'Theaters': {
            'name': lambda fake, row: fake.word()[:10],  # ensuring <= 10 chars
            'capacity': lambda fake, row: fake.random_int(min=1, max=199),
        },
        'Movies': {
            'duration': lambda fake, row: fake.random_int(min=60, max=200),
            'penalty_rate': lambda fake, row: float(fake.random_int(min=1, max=50)),
        },
        'Seats': {
            'row': lambda fake, row: fake.random_int(min=1, max=20),
            'seat': lambda fake, row: fake.random_int(min=1, max=25),
        },
        'Shows': {
            'show_date': lambda fake, row: fake.date_between(start_date='-40y', end_date='today'),
            'show_starts_at': lambda fake, row: fake.time(),
        },
        'Tickets': {
            'price': lambda fake, row: round(fake.random_number(digits=3, fix_len=False), 2),
        }
    }
    num_rows_per_table = {
        'Theaters': 5,
        'Seats': 50,
        'Movies': 10,
        'Shows': 20,
        'Tickets': 50,
    }

    return DataGenerator(
        tables=theater_tables_parsed,
        num_rows=10,
        predefined_values=predefined_values,
        column_type_mappings=column_type_mappings,
        num_rows_per_table=num_rows_per_table
    )


def test_parse_create_tables_theater(theater_tables_parsed):
    """Check that the theater schema is parsed properly."""
    assert len(theater_tables_parsed) > 0, "No tables parsed from theater_sql_script.sql"
    expected_tables = {"Theaters", "Seats", "Movies", "Shows", "Tickets"}
    assert expected_tables.issubset(theater_tables_parsed.keys()), (
        f"Missing expected tables. Found: {theater_tables_parsed.keys()}"
    )


def test_generate_data_theater(theater_data_generator):
    """Verify we get non-empty results for each table."""
    fake_data = theater_data_generator.generate_data()
    for table_name in theater_data_generator.tables.keys():
        assert table_name in fake_data, f"Missing data for table {table_name}"
        assert len(fake_data[table_name]) > 0, f"No rows generated for table {table_name}"


def test_export_sql_theater(theater_data_generator):
    """Basic check that the generated SQL has insert statements and references a known table."""
    theater_data_generator.generate_data()
    sql_output = theater_data_generator.export_as_sql_insert_query()
    assert "INSERT INTO" in sql_output
    assert "Theaters" in sql_output


def test_constraints_theater(theater_data_generator):
    """
    More advanced checks:
      - Theaters capacity
      - Movies duration, penalty_rate
      - Seats uniqueness (row, seat, theater_id)
      - Shows references Theaters, Movies
      - Tickets references Shows and Seats
    """
    data = theater_data_generator.generate_data()

    # 1) Theaters
    theater_ids = set()
    for t in data.get("Theaters", []):
        tid = t["theater_id"]
        theater_ids.add(tid)
        cap = t["capacity"]
        assert 0 < cap < 200, f"Theater capacity out of range: {cap}"
        assert len(t["name"]) <= 10, f"Theater name too long: {t['name']}"

    # 2) Movies
    movie_ids = set()
    for m in data.get("Movies", []):
        mid = m["movie_id"]
        movie_ids.add(mid)
        dur = m["duration"]
        assert 60 <= dur <= 200, f"Movie duration out of range: {dur}"
        assert m["penalty_rate"] > 0, f"penalty_rate must be > 0, got {m['penalty_rate']}"

    # 3) Seats
    seats_set = set()
    for s in data.get("Seats", []):
        seat_key = (s["row"], s["seat"], s["theater_id"])
        # Uniqueness check
        assert seat_key not in seats_set, f"Duplicate seat found: {seat_key}"
        seats_set.add(seat_key)
        # The theater_id must exist in Theaters
        assert s["theater_id"] in theater_ids, f"Invalid theater_id {s['theater_id']} in Seats"

    # 4) Shows: references Theaters, Movies
    show_ids = set()
    for show in data.get("Shows", []):
        sid = show["show_id"]
        show_ids.add(sid)

        # FKs
        assert show["theater_id"] in theater_ids, f"Invalid theater_id {show['theater_id']} in Shows"
        assert show["movie_id"] in movie_ids, f"Invalid movie_id {show['movie_id']} in Shows"

        # Basic check for show_date and show_starts_at being not None
        assert show["show_date"], "show_date is missing"
        assert show["show_starts_at"], "show_starts_at is missing"

    # 5) Tickets: references Shows(show_id) and Seats(row, seat, theater_id)
    for tk in data.get("Tickets", []):
        # 5a) Price >= 0
        assert tk["price"] >= 0, f"Ticket price should be >= 0, got {tk['price']}"
        # 5b) show_id must exist
        assert tk["show_id"] in show_ids, f"Invalid show_id {tk['show_id']} in Tickets"

        # 5c) (row, seat, theater_id) must exist in Seats
        seat_key = (tk["row"], tk["seat"], tk["theater_id"])
        assert seat_key in seats_set, (
            f"Invalid seat reference {seat_key} in Tickets"
        )