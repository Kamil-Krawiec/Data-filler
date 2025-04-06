import pytest
from datetime import date, datetime
from parsing import parse_create_tables
from filling import DataGenerator


def test_complex_sql_schema():
    """
    Test complex, production-like SQL schema.

    The schema includes:
      - Numeric constraints (e.g. established_year, order_quantity, rating)
      - Date constraints (e.g. issue_date, order_date, review_date)
      - String constraints (e.g. country non-empty, series_name LIKE 'Series_%', review_text length)
      - Composite primary keys and multiple foreign keys with ON DELETE options

    This test explicitly asserts that each generated row meets all constraints.
    """
    sql_script = """
    CREATE TABLE Publishers (
        publisher_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        country VARCHAR(50) NOT NULL CHECK (country <> ''),
        established_year INT CHECK (established_year >= 1500 AND established_year <= EXTRACT(YEAR FROM CURRENT_DATE))
    );

    CREATE TABLE Series (
        series_id SERIAL PRIMARY KEY,
        publisher_id INT NOT NULL,
        series_name VARCHAR(100) NOT NULL,
        start_year INT NOT NULL CHECK (start_year >= 1900),
        end_year INT CHECK (end_year IS NULL OR end_year >= start_year),
        FOREIGN KEY (publisher_id) REFERENCES Publishers(publisher_id) ON DELETE CASCADE,
        CHECK (series_name LIKE 'Series_%')
    );

    CREATE TABLE Volumes (
        volume_num INT NOT NULL,
        series_id INT NOT NULL,
        volume_title VARCHAR(200) NOT NULL,
        issue_date DATE NOT NULL CHECK (issue_date > '1900-01-01'),
        PRIMARY KEY (volume_num, series_id),
        FOREIGN KEY (series_id) REFERENCES Series(series_id) ON DELETE CASCADE
    );

    CREATE TABLE Orders (
        order_id SERIAL PRIMARY KEY,
        volume_num INT NOT NULL,
        series_id INT NOT NULL,
        order_quantity INT NOT NULL CHECK (order_quantity > 0),
        order_date DATE NOT NULL CHECK (order_date BETWEEN '2000-01-01' AND CURRENT_DATE),
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

    CREATE TABLE Reviews (
        review_id SERIAL PRIMARY KEY,
        book_id INT NOT NULL,
        rating DECIMAL(3,1) CHECK (rating BETWEEN 0.0 AND 5.0),
        review_text TEXT,
        review_date DATE NOT NULL CHECK (review_date >= '2000-01-01'),
        FOREIGN KEY (book_id) REFERENCES Books(book_id) ON DELETE CASCADE,
        CHECK (LENGTH(review_text) >= 10)
    );
    """
    # Parse the schema and generate data.
    tables = parse_create_tables(sql_script)
    generator = DataGenerator(tables, num_rows=100)
    data = generator.generate_data(run_repair=True, print_stats=False)
    current_year = date.today().year

    # --- Publishers ---
    for row in data["Publishers"]:
        # publisher_id must be positive integer.
        assert isinstance(row["publisher_id"], int) and row["publisher_id"] > 0
        # Name must be non-empty.
        assert row["name"]
        # Country must be non-empty.
        assert row["country"] != ''
        # established_year must be between 1500 and current year.
        assert row["established_year"] >= 1500
        assert row["established_year"] <= current_year

    # --- Series ---
    publisher_ids = {r["publisher_id"] for r in data["Publishers"]}
    for row in data["Series"]:
        # series_id must be positive.
        assert isinstance(row["series_id"], int) and row["series_id"] > 0
        # publisher_id must exist in Publishers.
        assert row["publisher_id"] in publisher_ids
        # series_name must start with 'Series_'.
        assert row["series_name"].startswith("Series_")
        # start_year must be >= 1900.
        assert row["start_year"] >= 1900
        # end_year, if not null, must be >= start_year.
        if row.get("end_year") is not None:
            assert row["end_year"] >= row["start_year"]

    # --- Volumes ---
    series_ids = {r["series_id"] for r in data["Series"]}
    for row in data["Volumes"]:
        # volume_num is an integer.
        assert isinstance(row["volume_num"], int)
        # series_id must exist in Series.
        assert row["series_id"] in series_ids
        # volume_title must be non-empty.
        assert row["volume_title"]
        # issue_date must be after 1900-01-01.
        assert isinstance(row["issue_date"], date)
        assert row["issue_date"] > date(1900, 1, 1)

    # --- Orders ---
    volume_keys = {(r["volume_num"], r["series_id"]) for r in data["Volumes"]}
    for row in data["Orders"]:
        assert isinstance(row["order_id"], int)
        # order_quantity must be > 0.
        assert row["order_quantity"] > 0
        # order_date between 2000-01-01 and today.
        assert isinstance(row["order_date"], date)
        assert row["order_date"] >= date(2000, 1, 1)
        assert row["order_date"] <= date.today()
        # Foreign key: (volume_num, series_id) must exist in Volumes.
        assert (row["volume_num"], row["series_id"]) in volume_keys

    # --- Authors ---
    for row in data["Authors"]:
        assert isinstance(row["author_id"], int) and row["author_id"] > 0
        assert row["first_name"]
        assert row["last_name"]
        assert row["birth_year"] > 1800

    # --- Books ---
    for row in data["Books"]:
        assert isinstance(row["book_id"], int) and row["book_id"] > 0
        assert row["title"]
        pub_year = row["publication_year"]
        assert pub_year >= 1900 and pub_year <= current_year
        if row.get("series_id") is not None:
            series_ids = {r["series_id"] for r in data["Series"]}
            assert row["series_id"] in series_ids

    # --- BookAuthors ---
    book_ids = {r["book_id"] for r in data["Books"]}
    author_ids = {r["author_id"] for r in data["Authors"]}
    for row in data["BookAuthors"]:
        # Composite primary key: both must exist.
        assert row["book_id"] in book_ids
        assert row["author_id"] in author_ids

    # --- Reviews ---
    for row in data["Reviews"]:
        assert isinstance(row["review_id"], int) and row["review_id"] > 0
        assert row["book_id"] in book_ids
        # rating must be between 0.0 and 5.0.
        rating = float(row["rating"])
        assert 0.0 <= rating <= 5.0
        # review_text must be at least 10 characters long.
        if row["review_text"]:
            assert len(row["review_text"]) >= 10
        # review_date must be on or after 2000-01-01.
        assert isinstance(row["review_date"], date)
        assert row["review_date"] >= date(2000, 1, 1)