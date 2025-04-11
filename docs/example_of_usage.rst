Intelligent Data Generator – Updated Usage Guide
==================================================

This guide demonstrates how to utilize the Intelligent Data Generator with a new SQL schema and additional features. In this updated example, you will see:

- A new SQL schema with tables such as Shops, Categories, Products, Orders, OrderItems, Coupons, and CouponUsages.
- Automatic guessing of column type mappings using fuzzy matching.
- A preview option to inspect the inferred mappings.
- Flexible export options (CSV, JSON, or SQL).

Prerequisites
-------------

Before starting, ensure you have:

- Installed the package using::

  pip install intelligent-data-generator

- A Python 3.10+ environment.

Step 1: Import Required Modules
-------------------------------

Begin by importing the necessary modules. Notice the new import for the ``ColumnMappingsGenerator``:

.. code-block:: python

    from parsing.parsing import parse_create_tables
    from filling.data_generator import DataGenerator
    from filling.column_mappings_generator import ColumnMappingsGenerator
    import pprint  # Optional: for pretty-printing generated data

Step 2: Define and Parse the SQL Script
----------------------------------------

The following SQL script defines a new schema with multiple related tables:

.. code-block:: python

    sql_script = """
        CREATE TABLE Shops (
            shop_id SERIAL PRIMARY KEY,
            shop_name VARCHAR(100) NOT NULL CHECK (shop_name <> ''),
            country VARCHAR(50) CHECK (country IN ('USA','CANADA','MEXICO','OTHER')),
            established_year INT CHECK (established_year >= 1900 AND established_year <= EXTRACT(YEAR FROM CURRENT_DATE))
        );

        CREATE TABLE Categories (
            category_id SERIAL PRIMARY KEY,
            category_name VARCHAR(50) NOT NULL CHECK (shop_name <> ''),
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
    tables_parsed = parse_create_tables(sql_script)

Step 3: Initialize the Data Generator with New Features
--------------------------------------------------------

Create an instance of the ``DataGenerator`` with the following new options:

- **Automatic Column Mapping Guessing:** Set ``guess_column_type_mappings=True`` to use fuzzy matching via the ``ColumnMappingsGenerator``.
- **Threshold for Guessing:** The ``threshold_for_guessing`` parameter (set here to 95) adjusts the sensitivity of the fuzzy matching.
- **Preview Inferred Mappings:** Use ``preview_inferred_mappings()`` to generate a small sample of rows to inspect the inferred column mappings.

.. code-block:: python

    # Create an instance of DataGenerator with automatic mapping guessing enabled
    data_generator = DataGenerator(
        tables_parsed,
        num_rows=1000,
        guess_column_type_mappings=True,
        threshold_for_guessing=95
    )

    # Preview the inferred column mappings (showing a sample of generated rows for each table)
    data_generator.preview_inferred_mappings()

    # Generate the synthetic data
    fake_data = data_generator.generate_data()

Step 4: Export the Generated Data
----------------------------------

The DataGenerator now supports exporting generated data in multiple file formats:

- **CSV Export:** Exports each table’s data to individual CSV files.
- **JSON Export:** Exports each table’s data to individual JSON files.
- **SQL Export:** By default, if no file type is explicitly provided, data will be exported as a single SQL file containing INSERT statements.

.. code-block:: python

    # Export data as CSV files
    data_generator.export_data_files('fake_data', 'CSV')

    # Export data as JSON files
    data_generator.export_data_files('fake_data', 'JSON')

    # Export data as a SQL file (default when file type is not specified)
    data_generator.export_data_files('fake_data')

Complete Example Script
-----------------------

Below is the complete script that ties together all the steps and new features:

.. code-block:: python

    from parsing import parse_create_tables
    from filling import DataGenerator,ColumnMappingsGenerator
    import pprint

    # Define and parse the SQL schema
    sql_script = """
        CREATE TABLE Shops (
            shop_id SERIAL PRIMARY KEY,
            shop_name VARCHAR(100) NOT NULL CHECK (shop_name <> ''),
            country VARCHAR(50) CHECK (country IN ('USA','CANADA','MEXICO','OTHER')),
            established_year INT CHECK (established_year >= 1900 AND established_year <= EXTRACT(YEAR FROM CURRENT_DATE))
        );

        CREATE TABLE Categories (
            category_id SERIAL PRIMARY KEY,
            category_name VARCHAR(50) NOT NULL CHECK (shop_name <> ''),
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
    tables_parsed = parse_create_tables(sql_script)

    # Create DataGenerator instance with automatic mapping guessing enabled
    data_generator = DataGenerator(
        tables_parsed,
        num_rows=1000,
        guess_column_type_mappings=True,
        threshold_for_guessing=95
    )

    # Preview inferred column mappings (sample output for each table)
    data_generator.preview_inferred_mappings()

    # Generate synthetic data
    fake_data = data_generator.generate_data()

    # Export generated data in multiple formats
    data_generator.export_data_files('fake_data', 'CSV')
    data_generator.export_data_files('fake_data', 'JSON')
    data_generator.export_data_files('fake_data')

    # Optional: Pretty-print a portion of the generated data
    pprint.pprint(fake_data)

Additional Guides
------------------

This section provides extra guides on creating custom mappings and advanced customization options for the Intelligent Data Generator.

Column Mappings Creation Guide
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Column mappings are critical for aligning synthetic data with your database schema. The Intelligent Data Generator automatically guesses mappings using fuzzy matching, but you can create custom mappings to override the defaults.

**Overview:**

- *Default Mappings:* Automatically generated based on column names and types.
- *Custom Mappings:* Define your own mapping dictionary to provide specific generators for each column.

**Creating Custom Mappings:**

1. Define a Python dictionary with column names as keys and generator functions as values.
2. Pass this dictionary to the `ColumnMappingsGenerator` when initializing the DataGenerator.

Example:

.. code-block:: python

    import random
    from filling import ColumnMappingsGenerator,DataGenerator

    custom_mappings = {
        'shop_name': lambda: 'Shop ' + str(random.randint(1, 100)),
        'country': lambda: random.choice(['USA', 'CANADA', 'MEXICO']),
        'established_year': lambda: random.randint(1950, 2022),
    }

    column_mapper = ColumnMappingsGenerator(custom_mappings=custom_mappings)

    data_generator = DataGenerator(
        tables_parsed,
        num_rows=500,
        guess_column_type_mappings=False,  # Disable default guessing
        custom_column_mapper=column_mapper
    )

**Tips:**

- Use descriptive keys to match your schema.
- Test your mappings using `preview_inferred_mappings()` before generating full datasets.
- Customize generator functions to meet specific data constraints.

Advanced Data Generation Customization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Beyond custom mappings, you can further tailor the data generation process by adjusting parameters such as the number of rows, enforcing data constraints, and applying post-generation transformations.

Data Export and Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The Intelligent Data Generator supports exporting data in multiple formats:

- **CSV:** Ideal for spreadsheet analysis and databases that support CSV imports.
- **JSON:** Useful for web applications and NoSQL databases.
- **SQL:** Generates INSERT statements for quickly populating SQL databases.

Choose the appropriate export method based on your integration needs.

Troubleshooting and FAQs
~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Mappings Not Being Applied:**
  Verify that your custom mappings dictionary uses the correct column names and that the `guess_column_type_mappings` flag is set as needed.

- **Data Constraint Violations:**
  Ensure that your mapping functions generate values that satisfy the SQL constraints defined in your schema.

- **Preview Issues:**
  Use the `preview_inferred_mappings()` method to inspect sample data and adjust your mappings accordingly.

Conclusion
~~~~~~~~~~
These additional guides are designed to help you customize and extend the functionality of the Intelligent Data Generator to best fit your project requirements. Use them as a reference to create more precise and realistic synthetic data tailored to your needs.
