CREATE TABLE Customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    phone VARCHAR(15),
    registration_date DATE NOT NULL DEFAULT CURRENT_DATE,

    CONSTRAINT chk_email_format
        CHECK (email ~ '^[\w\.-]+@[\w\.-]+\.\w{2,}$')
);

CREATE TABLE Products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL UNIQUE,
    price DECIMAL(10,2) NOT NULL,
    stock_quantity INT NOT NULL,

    CONSTRAINT chk_price_positive
        CHECK (price > 0),
    CONSTRAINT chk_stock_non_negative
        CHECK (stock_quantity >= 0)
);

CREATE TABLE Orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL DEFAULT CURRENT_DATE,
    total_amount DECIMAL(10,2) NOT NULL,

    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id)
        REFERENCES Customers(customer_id),

    CONSTRAINT chk_total_amount
        CHECK (total_amount >= 0)
);

CREATE TABLE OrderItems (
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,

    CONSTRAINT pk_orderitems
        PRIMARY KEY (order_id, product_id),

    CONSTRAINT fk_orderitems_order
        FOREIGN KEY (order_id)
        REFERENCES Orders(order_id),

    CONSTRAINT fk_orderitems_product
        FOREIGN KEY (product_id)
        REFERENCES Products(product_id),

    CONSTRAINT chk_quantity_positive
        CHECK (quantity > 0),

    CONSTRAINT chk_price_positive
        CHECK (price > 0)
);

CREATE TABLE Suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(100) NOT NULL UNIQUE,
    contact_name VARCHAR(50),
    contact_email VARCHAR(100),

    CONSTRAINT chk_contact_email_format
        CHECK (contact_email IS NULL OR contact_email ~ '^[\w\.-]+@[\w\.-]+\.\w{2,}$')
);

CREATE TABLE ProductSuppliers (
    product_id INT NOT NULL,
    supplier_id INT NOT NULL,
    supply_price DECIMAL(10,2) NOT NULL,

    CONSTRAINT pk_products_suppliers
        PRIMARY KEY (product_id, supplier_id),

    CONSTRAINT fk_products_suppliers_product
        FOREIGN KEY (product_id)
        REFERENCES Products(product_id),

    CONSTRAINT fk_products_suppliers_supplier
        FOREIGN KEY (supplier_id)
        REFERENCES Suppliers(supplier_id),

    CONSTRAINT chk_supply_price_positive
        CHECK (supply_price > 0)
);