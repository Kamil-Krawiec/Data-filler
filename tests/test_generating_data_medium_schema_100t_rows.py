import pytest
import time
import logging
from datetime import date

from parsing import parse_create_tables
from filling import DataGenerator

@pytest.fixture
def medium_schema_sql():
    """
    A 'medium' complexity schema with simpler constraints but
    larger row generation (around 100K in the biggest table).
    """
    sql = """
    CREATE TABLE Customers (
        customer_id SERIAL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        signup_date DATE NOT NULL,
        CHECK (signup_date >= '2000-01-01')
    );

    CREATE TABLE Products (
        product_id SERIAL PRIMARY KEY,
        product_name VARCHAR(100) NOT NULL,
        category VARCHAR(50) NOT NULL,
        price DECIMAL(10,2) NOT NULL CHECK (price > 0)
    );

    CREATE TABLE Orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INT NOT NULL,
        order_date DATE NOT NULL,
        FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
    );

    CREATE TABLE Order_Items (
        order_id INT NOT NULL,
        product_id INT NOT NULL,
        quantity INT NOT NULL CHECK (quantity >= 1),
        PRIMARY KEY (order_id, product_id),
        FOREIGN KEY (order_id) REFERENCES Orders(order_id),
        FOREIGN KEY (product_id) REFERENCES Products(product_id)
    );
    """
    return sql

@pytest.fixture
def medium_schema_tables(medium_schema_sql):
    return parse_create_tables(medium_schema_sql, dialect='postgres')

@pytest.fixture
def medium_data_generator(medium_schema_tables):
    """
    We will target ~100K rows in the largest table.
    """
    num_rows_per_table = {
        "Customers": 30000,
        "Products": 5000,
        "Orders": 50000,
        "Order_Items": 100000  # largest table
    }

    column_type_mappings = {
        "Customers": {
            "first_name": "first_name",
            "last_name": "last_name",
            "signup_date": lambda fake, row: fake.date_between(start_date="-10y", end_date="today")
        },
        "Products": {
            "product_name": lambda fake, row: f"{fake.word().capitalize()} {fake.word().capitalize()}",
            "category": lambda fake, row: fake.random_element(elements=["Books","Electronics","Clothing","Household"]),
            "price": lambda fake, row: round(fake.random_number(digits=3) + fake.random.random(), 2)
        },
        "Orders": {
            "order_date": lambda fake, row: fake.date_between(start_date="-5y", end_date="today")
        },
        "Order_Items": {
            "quantity": lambda fake, row: fake.random_int(min=1, max=100)
        }
    }

    return DataGenerator(
        tables=medium_schema_tables,
        num_rows=5000,  # baseline if not overridden
        predefined_values=None,
        column_type_mappings=column_type_mappings,
        num_rows_per_table=num_rows_per_table
    )

def verify_medium_schema_data(data, logger):
    """
    Manually verify constraints for the 'medium' schema.
    Returns a dict with counts of each violation type and total.
    """

    # Gather parent PK sets for foreign key checks
    customers_pk = {row["customer_id"] for row in data.get("Customers", [])}
    products_pk = {row["product_id"] for row in data.get("Products", [])}
    orders_pk = set()

    violations = {
        "not_null": 0,
        "check_constraint": 0,
        "fk_violation": 0,
        "pk_violation": 0
    }

    # Customers: PK=customer_id
    # Not null: first_name, last_name, signup_date
    # Check: signup_date >= '2000-01-01'
    # We'll just trust that the user_id is unique because it's serial, but let's do a quick set check if we want.
    cust_ids = set()
    for row in data.get("Customers", []):
        if (row["first_name"] is None or
            row["last_name"] is None or
            row["signup_date"] is None):
            violations["not_null"] += 1
        if row["customer_id"] in cust_ids:
            violations["pk_violation"] += 1
        else:
            cust_ids.add(row["customer_id"])
        if row["signup_date"] < date(2000, 1, 1):
            violations["check_constraint"] += 1

    # Products: PK=product_id
    # Not null: product_name, category, price
    # Check: price > 0
    prod_ids = set()
    for row in data.get("Products", []):
        if (row["product_name"] is None or
            row["category"] is None or
            row["price"] is None):
            violations["not_null"] += 1
        if row["product_id"] in prod_ids:
            violations["pk_violation"] += 1
        else:
            prod_ids.add(row["product_id"])
        if row["price"] <= 0:
            violations["check_constraint"] += 1

    # Orders: PK=order_id
    # Not null: customer_id, order_date
    # FK: customer_id in customers_pk
    # We'll store order_id in orders_pk for Order_Items
    for row in data.get("Orders", []):
        oid = row["order_id"]
        if row["customer_id"] is None or row["order_date"] is None:
            violations["not_null"] += 1
        if oid in orders_pk:
            violations["pk_violation"] += 1
        else:
            orders_pk.add(oid)
        if row["customer_id"] not in customers_pk:
            violations["fk_violation"] += 1

    # Order_Items: PK=(order_id, product_id)
    # Not null: quantity
    # quantity >=1
    # FK: order_id in orders_pk, product_id in products_pk
    item_pk = set()
    for row in data.get("Order_Items", []):
        pk_tuple = (row["order_id"], row["product_id"])
        if pk_tuple in item_pk:
            violations["pk_violation"] += 1
        else:
            item_pk.add(pk_tuple)

        if row["quantity"] is None:
            violations["not_null"] += 1
        elif row["quantity"] < 1:
            violations["check_constraint"] += 1

        if row["order_id"] not in orders_pk:
            violations["fk_violation"] += 1
        if row["product_id"] not in prod_ids:
            violations["fk_violation"] += 1

    total_violations = sum(violations.values())
    return {
        "not_null": violations["not_null"],
        "check_constraint": violations["check_constraint"],
        "fk_violation": violations["fk_violation"],
        "pk_violation": violations["pk_violation"],
        "total": total_violations
    }


def test_medium_schema_generation(medium_data_generator):
    """
    Generate data for a 'medium' complexity schema (100K+ rows in total),
    manually verify constraints, and log results in a table.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # With repair
    start_repair = time.time()
    data_with_repair = medium_data_generator.generate_data(run_repair=True, print_stats=False)
    elapsed_repair = time.time() - start_repair

    repair_violations = verify_medium_schema_data(data_with_repair, logger)

    # Without repair
    start_norepair = time.time()
    data_without_repair = medium_data_generator.generate_data(run_repair=False, print_stats=False)
    elapsed_norepair = time.time() - start_norepair

    norepair_violations = verify_medium_schema_data(data_without_repair, logger)

    # Print summary in table format
    print("\n===== MEDIUM SCHEMA TEST RESULTS =====")
    print("{:<35} | {:>12} | {:>12}".format("Metric", "With Repair", "No Repair"))
    print("-" * 66)
    print("{:<35} | {:>12.2f} | {:>12.2f}".format("Generation Time (s)", elapsed_repair, elapsed_norepair))
    print("{:<35} | {:>12} | {:>12}".format("Not Null Violations", repair_violations["not_null"], norepair_violations["not_null"]))
    print("{:<35} | {:>12} | {:>12}".format("Check Violations", repair_violations["check_constraint"], norepair_violations["check_constraint"]))
    print("{:<35} | {:>12} | {:>12}".format("FK Violations", repair_violations["fk_violation"], norepair_violations["fk_violation"]))
    print("{:<35} | {:>12} | {:>12}".format("PK/Unique Violations", repair_violations["pk_violation"], norepair_violations["pk_violation"]))
    print("{:<35} | {:>12} | {:>12}".format("TOTAL Violations", repair_violations["total"], norepair_violations["total"]))
    print("-" * 66)

    assert repair_violations["total"] == 0, (
        f"Found {repair_violations['total']} total violations after repair."
    )