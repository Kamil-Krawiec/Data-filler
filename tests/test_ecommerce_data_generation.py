import os
import re
import pytest
import random
from datetime import date

from parsing import parse_create_tables
from filling import DataGenerator


@pytest.fixture
def ecommerce_sql_script_path():
    """
    Provide the path to the E-commerce schema .sql file.
    """
    return os.path.join("tests", "DB_infos/ecommerce_sql_script.sql")


@pytest.fixture
def ecommerce_sql_script(ecommerce_sql_script_path):
    with open(ecommerce_sql_script_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def ecommerce_tables_parsed(ecommerce_sql_script):
    """
    Parse the CREATE TABLE statements from the ecommerce SQL script.
    Returns a dict of table definitions.
    """
    return parse_create_tables(ecommerce_sql_script)


@pytest.fixture
def ecommerce_data_generator(ecommerce_tables_parsed):
    """
    Returns a DataGenerator instance configured for the E-commerce schema.
    """
    predefined_values = {
        'Products': {
            'product_name': [
                'Laptop', 'Smartphone', 'Headphones', 'Camera', 'Tablet',
                'Smartwatch', 'Printer', 'Monitor', 'Keyboard', 'Mouse',
            ]
        },
        'Suppliers': {
            'supplier_name': [
                'TechCorp', 'GadgetSupply', 'ElectroGoods', 'DeviceHub', 'AccessoryWorld'
            ]
        }
    }
    column_type_mappings = {
        'global': {
            'first_name': 'first_name',
            'last_name': 'last_name',
            'email': 'email',
            'phone': lambda fake, row: fake.phone_number()[:15],
        },
        'Customers': {
            'registration_date': lambda fake, row: fake.date_between(start_date='-5y', end_date='today'),
        },
        'Suppliers': {
            'contact_name': 'name',
            'contact_email': 'email',
        },
        'Orders': {
            'order_date': lambda fake, row: fake.date_between(start_date='-2y', end_date='today'),
        },
        'Products': {
            'price': lambda fake, row: round(random.uniform(5, 2000), 2),
            'stock_quantity': lambda fake, row: random.randint(0, 500),
        },
        'ProductSuppliers': {
            'supply_price': lambda fake, row: round(random.uniform(1, 1000), 2),
        }
    }
    num_rows_per_table = {
        'Customers': 50,
        'Products': 10,
        'Orders': 20,
        'OrderItems': 50,
        'Suppliers': 5,
        'ProductSuppliers': 20,
    }

    return DataGenerator(
        tables=ecommerce_tables_parsed,
        num_rows=10,  # fallback if not in num_rows_per_table
        predefined_values=predefined_values,
        column_type_mappings=column_type_mappings,
        num_rows_per_table=num_rows_per_table
    )


def test_parse_create_tables_ecommerce(ecommerce_tables_parsed):
    """Verify that the E-commerce script is parsed correctly."""
    assert len(ecommerce_tables_parsed) > 0, "No tables parsed from ecommerce_sql_script.sql"
    expected_tables = {"Customers", "Products", "Orders", "OrderItems", "Suppliers", "ProductSuppliers"}
    assert expected_tables.issubset(ecommerce_tables_parsed.keys()), (
        f"Missing expected tables. Found: {ecommerce_tables_parsed.keys()}"
    )


def test_generate_data_ecommerce(ecommerce_data_generator):
    """Test that generating data returns non-empty results for each table."""
    fake_data = ecommerce_data_generator.generate_data()
    for table_name in ecommerce_data_generator.tables.keys():
        assert table_name in fake_data, f"Missing data for table {table_name}"
        assert len(fake_data[table_name]) > 0, f"No rows generated for table {table_name}"


def test_export_sql_ecommerce(ecommerce_data_generator):
    """Basic check that exported SQL insert statements contain expected syntax."""
    ecommerce_data_generator.generate_data()
    sql_output = ecommerce_data_generator.export_as_sql_insert_query()
    assert "INSERT INTO" in sql_output, "SQL output missing 'INSERT INTO' statement"
    # Spot-check: ensure a known table appears in the final query
    assert "Products" in sql_output, "Expected 'Products' table not found in SQL output"


def test_constraints_ecommerce(ecommerce_data_generator):
    """
    Check some constraints in the e-commerce domain:
      - Email format
      - price > 0, quantity >= 0
      - total_amount >= 0
      - OrderItems references valid Orders and Products
      - ProductSuppliers references valid Products and Suppliers
    """
    data = ecommerce_data_generator.generate_data()

    # 1) Customers: check email format
    for cust in data.get("Customers", []):
        email = cust.get("email")
        assert re.match(r'^[\w\.-]+@[\w\.-]+\.\w{2,}$', email), f"Invalid email {email}"

    # 2) Products: check price > 0, stock >= 0
    product_ids = set()
    for prod in data.get("Products", []):
        pid = prod["product_id"]
        product_ids.add(pid)

        price = prod.get("price")
        stock = prod.get("stock_quantity")
        assert price > 0, f"Product price must be > 0, got {price}"
        assert stock >= 0, f"Stock quantity must be >= 0, got {stock}"

    # 3) Orders: total_amount >= 0, check foreign key to Customers
    #    We'll gather all valid customer_ids
    customer_ids = {row["customer_id"] for row in data.get("Customers", [])}
    order_ids = set()
    for order in data.get("Orders", []):
        oid = order["order_id"]
        order_ids.add(oid)

        assert order.get("total_amount") >= 0, f"total_amount must be >= 0, got {order['total_amount']}"
        # Check that the order's customer_id references a valid customer
        assert order["customer_id"] in customer_ids, (
            f"Invalid customer_id {order['customer_id']} in Orders"
        )

    # 4) OrderItems: quantity > 0, price > 0, references valid Orders & Products
    for oi in data.get("OrderItems", []):
        assert oi["quantity"] > 0, f"OrderItem quantity must be > 0, got {oi['quantity']}"
        assert oi["price"] > 0, f"OrderItem price must be > 0, got {oi['price']}"
        assert oi["order_id"] in order_ids, f"Invalid order_id {oi['order_id']} in OrderItems"
        assert oi["product_id"] in product_ids, f"Invalid product_id {oi['product_id']} in OrderItems"

    # 5) Suppliers + ProductSuppliers: supply_price > 0, references valid product_id & supplier_id
    supplier_ids = {row["supplier_id"] for row in data.get("Suppliers", [])}
    for ps in data.get("ProductSuppliers", []):
        assert ps["product_id"] in product_ids, f"Invalid product_id {ps['product_id']} in ProductSuppliers"
        assert ps["supplier_id"] in supplier_ids, f"Invalid supplier_id {ps['supplier_id']} in ProductSuppliers"
        assert ps["supply_price"] > 0, f"supply_price must be > 0, got {ps['supply_price']}"