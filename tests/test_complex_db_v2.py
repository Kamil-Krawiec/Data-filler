import pytest
from datetime import date, datetime
from parsing import parse_create_tables
from filling import DataGenerator

def test_alternative_sql_schema():
    """
    Test an alternative, complex SQL schema with multiple tables, constraints,
    foreign keys, numeric ranges, date checks, and text constraints.
    This is intended to verify that DataGenerator respects the constraints
    in a different scenario from the original 'Publishers/Series/Volumes/...'.
    """

    sql_script = """
    CREATE TABLE Shops (
        shop_id SERIAL PRIMARY KEY,
        shop_name VARCHAR(100) NOT NULL CHECK (shop_name <> ''),
        country VARCHAR(50) CHECK (country IN ('USA','CANADA','MEXICO','OTHER')),
        established_year INT CHECK (established_year >= 1900 AND established_year <= EXTRACT(YEAR FROM CURRENT_DATE))
    );

    CREATE TABLE Categories (
        category_id SERIAL PRIMARY KEY,
        category_name VARCHAR(50) NOT NULL CHECK (category_name <> ''),
        description TEXT CHECK (LENGTH(description) >= 10)
    );

    CREATE TABLE Products (
        product_id SERIAL PRIMARY KEY,
        shop_id INT NOT NULL,
        category_id INT NOT NULL,
        product_name VARCHAR(100) NOT NULL,
        price DECIMAL(8,2) CHECK (price > 0.0),
        FOREIGN KEY (shop_id) REFERENCES Shops(shop_id) ON DELETE CASCADE,
        FOREIGN KEY (category_id) REFERENCES Categories(category_id) ON DELETE CASCADE
    );

    CREATE TABLE Orders (
        order_id SERIAL PRIMARY KEY,
        shop_id INT NOT NULL,
        order_date DATE NOT NULL CHECK (order_date >= '2010-01-01'),
        total_amount DECIMAL(10,2) NOT NULL CHECK (total_amount >= 0),
        FOREIGN KEY (shop_id) REFERENCES Shops(shop_id) ON DELETE RESTRICT
    );

    CREATE TABLE OrderItems (
        order_id INT NOT NULL,
        product_id INT NOT NULL,
        quantity INT NOT NULL CHECK (quantity > 0),
        PRIMARY KEY (order_id, product_id),
        FOREIGN KEY (order_id) REFERENCES Orders(order_id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES Products(product_id) ON DELETE CASCADE
    );

    CREATE TABLE Coupons (
        coupon_id SERIAL PRIMARY KEY,
        code VARCHAR(20) NOT NULL,
        discount_rate DECIMAL(5,2) CHECK (discount_rate >= 0.00 AND discount_rate <= 99.99),
        valid_until DATE CHECK (valid_until >= CURRENT_DATE)
    );

    CREATE TABLE CouponUsages (
        coupon_id INT NOT NULL,
        order_id INT NOT NULL,
        PRIMARY KEY (coupon_id, order_id),
        FOREIGN KEY (coupon_id) REFERENCES Coupons(coupon_id) ON DELETE CASCADE,
        FOREIGN KEY (order_id) REFERENCES Orders(order_id) ON DELETE CASCADE
    );
    """

    # 1) Parse the schema
    tables = parse_create_tables(sql_script)

    # 2) Generate data
    generator = DataGenerator(tables, num_rows=50)
    data = generator.generate_data()

    current_year = date.today().year

    # ---- Shops ----
    for row in data["Shops"]:
        # shop_id must be positive
        assert isinstance(row["shop_id"], int) and row["shop_id"] > 0
        # shop_name must not be empty
        assert row["shop_name"] != ''
        # country must be one of 'USA','CANADA','MEXICO','OTHER' or possibly None if not set
        if row.get("country"):
            assert row["country"] in ('USA','CANADA','MEXICO','OTHER')
        # established_year between 1900 and current year
        assert 1900 <= row["established_year"] <= current_year

    # ---- Categories ----
    for row in data["Categories"]:
        assert isinstance(row["category_id"], int) and row["category_id"] > 0
        assert row["category_name"] != ''
        if row.get("description"):
            assert len(row["description"]) >= 10

    # ---- Products ----
    shop_ids = {r["shop_id"] for r in data["Shops"]}
    cat_ids = {r["category_id"] for r in data["Categories"]}
    for row in data["Products"]:
        assert isinstance(row["product_id"], int) and row["product_id"] > 0
        # shop_id must exist
        assert row["shop_id"] in shop_ids
        # category_id must exist
        assert row["category_id"] in cat_ids
        assert row["product_name"]
        price = float(row["price"])
        assert price > 0.0

    # ---- Orders ----
    for row in data["Orders"]:
        assert isinstance(row["order_id"], int) and row["order_id"] > 0
        # shop_id must exist
        assert row["shop_id"] in shop_ids
        # order_date >= 2010-01-01
        assert isinstance(row["order_date"], date)
        assert row["order_date"] >= date(2010, 1, 1)
        total_amt = float(row["total_amount"])
        assert total_amt >= 0

    # ---- OrderItems ----
    order_ids = {r["order_id"] for r in data["Orders"]}
    product_ids = {r["product_id"] for r in data["Products"]}
    for row in data["OrderItems"]:
        assert row["order_id"] in order_ids
        assert row["product_id"] in product_ids
        # quantity > 0
        assert row["quantity"] > 0

    # ---- Coupons ----
    for row in data["Coupons"]:
        assert isinstance(row["coupon_id"], int) and row["coupon_id"] > 0
        assert row["code"]
        discount_rate = float(row["discount_rate"])
        assert 0.00 <= discount_rate <= 99.99
        # valid_until >= today
        if row.get("valid_until"):
            valid_until = row["valid_until"]
            assert isinstance(valid_until, date)
            assert valid_until >= date.today()

    # ---- CouponUsages ----
    coupon_ids = {r["coupon_id"] for r in data["Coupons"]}
    for row in data["CouponUsages"]:
        # must exist in coupons
        assert row["coupon_id"] in coupon_ids
        # must exist in orders
        assert row["order_id"] in order_ids