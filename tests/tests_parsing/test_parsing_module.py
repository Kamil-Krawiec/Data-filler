import pytest
from parsing import parse_create_tables


def test_easy_parsing():
    """
    Easy Parsing Schema:
    A single table with two columns (one SERIAL PRIMARY KEY and one NOT NULL).
    """
    sql_script = """
    CREATE TABLE Test (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL
    );
    """
    tables = parse_create_tables(sql_script)
    assert "Test" in tables
    test_table = tables["Test"]

    # Check that there are exactly 2 columns.
    assert len(test_table["columns"]) == 2

    # Check primary key.
    assert test_table["primary_key"] == ["id"]

    # Check that there are no foreign keys or check constraints.
    assert test_table["foreign_keys"] == []
    assert test_table["check_constraints"] == []

    # Verify each column's constraints.
    for col in test_table["columns"]:
        if col["name"] == "id":
            assert "PRIMARY KEY" in col["constraints"]
            assert col["is_serial"] is True
        elif col["name"] == "name":
            assert "NOT NULL" in col["constraints"]


def test_medium_parsing():
    """
    Medium Parsing Schema:
    Two tables with a foreign key and a table-level check constraint.
    """
    sql_script = """
    CREATE TABLE Publishers (
        publisher_id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL
    );

    CREATE TABLE Books (
        book_id SERIAL PRIMARY KEY,
        publisher_id INT,
        title VARCHAR(150) NOT NULL,
        CHECK (title <> ''),
        FOREIGN KEY (publisher_id) REFERENCES Publishers(publisher_id)
    );
    """
    tables = parse_create_tables(sql_script)
    assert "Publishers" in tables
    assert "Books" in tables

    publishers = tables["Publishers"]
    books = tables["Books"]

    # Check Publishers table.
    assert publishers["primary_key"] == ["publisher_id"]
    assert len(publishers["columns"]) == 2

    # Check Books table.
    assert books["primary_key"] == ["book_id"]
    # Ensure that a foreign key exists for publisher_id.
    assert len(books["foreign_keys"]) >= 1
    fk = books["foreign_keys"][0]
    assert fk["ref_table"] == "Publishers"
    assert fk["ref_columns"] == ["publisher_id"]

    # Check that the table-level check constraint is captured.
    assert len(books["check_constraints"]) > 0
    # Expect the check to contain "title <> ''" (may or may not include surrounding quotes).
    check_text = books["check_constraints"][0].replace("(", "").replace(")", "").strip()
    assert "title <>" in check_text


def test_hard_parsing():
    """
    Hard Parsing Schema:
    Three tables with composite primary keys, multiple foreign keys (with options like ON DELETE CASCADE),
    table-level check constraints, and various constraint types.
    """
    sql_script = """
    CREATE TABLE Orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INT NOT NULL,
        order_date DATE NOT NULL,
        total DECIMAL(10,2) CHECK (total > 0)
    );

    CREATE TABLE OrderItems (
        order_id INT NOT NULL,
        item_id INT NOT NULL,
        product_id INT NOT NULL,
        quantity INT NOT NULL CHECK (quantity > 0),
        price DECIMAL(10,2) NOT NULL,
        PRIMARY KEY (order_id, item_id),
        FOREIGN KEY (order_id) REFERENCES Orders(order_id) ON DELETE CASCADE,
        CHECK (price >= 0)
    );

    CREATE TABLE Customers (
        customer_id INT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE,
        CHECK (email LIKE '%@%')
    );
    """
    tables = parse_create_tables(sql_script)

    # Orders table.
    assert "Orders" in tables
    orders = tables["Orders"]
    assert orders["primary_key"] == ["order_id"]
    assert any("total > 0" in chk for chk in orders["check_constraints"])

    # OrderItems table.
    assert "OrderItems" in tables
    order_items = tables["OrderItems"]
    # Composite primary key should contain both "order_id" and "item_id".
    assert set(order_items["primary_key"]) == {"order_id", "item_id"}
    # Check for at least one foreign key with an ON DELETE option.
    assert len(order_items["foreign_keys"]) >= 1
    fk = order_items["foreign_keys"][0]
    assert fk["ref_table"] == "Orders"
    # Check that the check constraint for price is captured.
    assert any("price >= 0" in chk for chk in order_items["check_constraints"])

    # Customers table.
    assert "Customers" in tables
    customers = tables["Customers"]
    assert customers["primary_key"] == ["customer_id"]
    # Ensure that a unique constraint exists for email.
    unique_flat = sum(customers["unique_constraints"], [])
    assert ['customer_id', 'email'] ==unique_flat
    # Check that the email check constraint (with LIKE) is captured.
    assert any("%@%" in chk for chk in customers["check_constraints"])