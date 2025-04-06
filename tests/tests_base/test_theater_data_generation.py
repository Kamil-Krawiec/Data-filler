import os
import re
import pytest
from parsing import parse_create_tables
from filling import DataGenerator


@pytest.fixture
def theater_sql_script_path():
    return os.path.join("tests/tests_base", "DB_infos/theater_sql_script.sql")


@pytest.fixture
def theater_sql_script(theater_sql_script_path):
    # with open(theater_sql_script_path, "r", encoding="utf-8") as f:
    #     return f.read()
    return """
    CREATE TABLE IF NOT EXISTS Theaters (
    theater_id SERIAL PRIMARY KEY,
    name VARCHAR(10) NOT NULL,
    capacity INT DEFAULT 0,

    CONSTRAINT capacity_positive CHECK (capacity>0 AND capacity<200)
);


CREATE TABLE Seats (
    row INT NOT NULL,
    seat INT NOT NULL,
    theater_id BIGINT UNSIGNED NOT NULL,
    CONSTRAINT unique_row_seat UNIQUE (row, seat, theater_id),
    CONSTRAINT fk_seats_theaters FOREIGN KEY (theater_id) REFERENCES Theaters(theater_id)
);


CREATE TABLE Movies (
    movie_id SERIAL PRIMARY KEY,
    title VARCHAR(100) NOT NULL,
    director VARCHAR(100) NOT NULL,
    duration INT NOT NULL,
    penalty_rate DECIMAL(5,2) NOT NULL CHECK (penalty_rate >= 0 AND penalty_rate <= 1),

    CONSTRAINT chk_duration
        CHECK (duration >= 60 AND duration <=200)

);

CREATE TABLE Shows (
    show_id SERIAL PRIMARY KEY,
    theater_id BIGINT UNSIGNED  NOT NULL,
    movie_id  BIGINT UNSIGNED NOT NULL,
    show_date date NOT NULL,
    show_starts_at time NOT NULL,

    CONSTRAINT fk_shows_movies
        FOREIGN KEY(movie_id)
        REFERENCES Movies(movie_id),

    CONSTRAINT fk_shows_theaters
        FOREIGN KEY(theater_id)
        REFERENCES Theaters(theater_id)

);

CREATE TABLE Tickets (
   show_id BIGINT UNSIGNED NOT NULL,
    row INT NOT NULL,
    seat INT NOT NULL,
    theater_id BIGINT UNSIGNED NOT NULL,
price decimal(8,2) DEFAULT 0,

  CONSTRAINT fk_tickets_shows FOREIGN KEY (show_id) REFERENCES Shows(show_id),
  CONSTRAINT fk_tickets_seat FOREIGN KEY (row, seat, theater_id) REFERENCES Seats(row, seat, theater_id),
  CONSTRAINT price_positive CHECK (price >= 0)
);
    """


@pytest.fixture
def theater_tables_parsed(theater_sql_script):
    return parse_create_tables(theater_sql_script)


@pytest.fixture
def theater_data_generator(theater_tables_parsed):
    """
    Returns a DataGenerator instance configured for the Theater schema.
    """
    predefined_values = {}


    return DataGenerator(
        tables=theater_tables_parsed,
        num_rows=100,
        predefined_values=predefined_values,
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
    Advanced checks for Theater schema:

    1) Theaters: capacity range, name length
    2) Movies: duration, penalty_rate
    3) Seats: row/seat + theater_id uniqueness
    4) Shows: references Theaters & Movies
    5) Tickets: references Shows(show_id) and composite seat (row, seat, theater_id)
    """
    data = theater_data_generator.generate_data()

    # 1) Theaters
    theater_ids = set()
    for t in data.get("Theaters", []):
        tid = t["theater_id"]
        theater_ids.add(tid)

        name = t["name"]
        assert 1 <= len(name) <= 10, f"Theater name must be between 1..10 chars, got '{name}'"
        cap = t["capacity"]
        assert 0 < cap < 200, f"Theater capacity out of range: {cap}"

    # 2) Movies
    movie_ids = set()
    for m in data.get("Movies", []):
        mid = m["movie_id"]
        movie_ids.add(mid)
        dur = m["duration"]
        rate = m["penalty_rate"]
        assert 60 <= dur <= 200, f"Movie duration out of range: {dur}"
        assert rate > 0, f"penalty_rate must be > 0, got {rate}"

    # 3) Seats => (row, seat, theater_id) unique + theater_id references Theaters
    seat_keys = set()
    for s in data.get("Seats", []):
        key = (s["row"], s["seat"], s["theater_id"])
        assert key not in seat_keys, f"Duplicate seat (row={key[0]}, seat={key[1]}, theater_id={key[2]})"
        seat_keys.add(key)
        # FK check
        assert s["theater_id"] in theater_ids, f"seat references nonexistent theater_id {s['theater_id']}"

    # 4) Shows => references Theaters + Movies
    show_ids = set()
    for sh in data.get("Shows", []):
        sid = sh["show_id"]
        show_ids.add(sid)

        assert sh["theater_id"] in theater_ids, f"Shows references nonexistent theater_id {sh['theater_id']}"
        assert sh["movie_id"] in movie_ids, f"Shows references nonexistent movie_id {sh['movie_id']}"

        # Basic check for show_date, show_starts_at
        assert sh["show_date"], "show_date is missing"
        assert sh["show_starts_at"], "show_starts_at is missing"

    # 5) Tickets => references Shows(show_id) + Seats(row, seat, theater_id)
    for tk in data.get("Tickets", []):
        # Price must be >= 0
        assert tk["price"] >= 0, f"Ticket price < 0, got {tk['price']}"

        # Check show_id in Show
        assert tk["show_id"] in show_ids, f"Ticket references nonexistent show_id {tk['show_id']}"

        # Check row/seat/theater_id in Seats
        seat_key = (tk["row"], tk["seat"], tk["theater_id"])
        assert seat_key in seat_keys, f"Ticket references nonexistent seat {seat_key}"