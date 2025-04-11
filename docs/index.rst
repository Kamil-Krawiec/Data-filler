Intelligent Data Generator Documentation
==========================================

Welcome to the **Intelligent Data Generator** documentation! This tool automates the creation and management of synthetic data tailored to your database schemas. Whether you’re developing, testing, or demonstrating your applications, our tool helps you generate realistic, constraint-compliant data quickly and efficiently.

Installation
------------

The Intelligent Data Generator is available on PyPI. To install, run:

.. code-block:: bash

    pip install intelligent-data-generator

Overview
--------

The Intelligent Data Generator consists of several key modules that work together seamlessly:

- **Schema Parser**
  Parses SQL scripts (from different dialects like PostgreSQL and MySQL) to extract table definitions, column types, constraints, foreign key relationships, and dependencies.

- **Data Filler**
  Generates synthetic data based on the parsed schema. It supports:

  - **Parallel Data Generation:**
    Tables are processed concurrently by grouping them by dependency level. This greatly accelerates data creation.

  - **Automatic Primary Key & Composite Key Generation:**
    Unique values and auto-increment behavior for SERIAL (or AUTO_INCREMENT) columns are ensured.

  - **Constraint Enforcement and Repair:**
    The tool checks for NOT NULL, UNIQUE, and CHECK constraints. If a generated row does not meet the criteria, it is either repaired or removed, ensuring data integrity.

  - **Foreign Key Handling:**
    Data for child tables is generated only after parent tables are populated, thus maintaining referential integrity.

- **Constraint Evaluator**
  With the help of the CheckConstraintEvaluator module, the generator parses and evaluates SQL CHECK constraints. It supports SQL functions (such as EXTRACT and DATE) and a variety of operators (e.g., BETWEEN, IN, LIKE).

- **Column Mappings with Fuzzy Matching**
  Optionally, the generator can auto-detect appropriate Faker methods for column names via fuzzy matching. This ensures that columns like “email”, “first_name”, or “birth_date” are populated with realistic data.

- **Flexible Data Export**
  Generated data can be exported to various formats:

  - **SQL INSERT Statements:**
    Data is split into manageable chunks to avoid database limits.
  - **CSV and JSON Files:**
    Each table’s data is exported into separate files.

Features
--------

- **Automated Schema Parsing:**
  Quickly parse complex SQL scripts and extract all necessary metadata for data generation.

- **Parallel Processing:**
  Data generation is distributed in parallel by table dependency levels to optimize performance.

- **Robust Constraint Enforcement:**
  The tool rigorously checks data integrity with built-in mechanisms to enforce NOT NULL, UNIQUE, and CHECK constraints, including custom repair logic.

- **Intelligent Value Generation:**
  Using the Faker library and fuzzy matching, the tool auto-maps column names to appropriate Faker methods (e.g., mapping “first_name” to `first_name()`, “email” to `email()`, etc.).
  It also supports ENUM types and IN constraints to generate values from fixed sets.

- **Flexible Export Options:**
  Export your generated data as SQL insert statements or as CSV/JSON files for easy integration into your testing or development environments.

Getting Started
---------------

A basic usage example:

.. code-block:: python

    from parsing import parse_create_tables
    from filling import DataGenerator,ColumnMappingsGenerator

    # Define a simple SQL script for parsing the schema
    sql_script = """
    CREATE TABLE Shops (
        shop_id SERIAL PRIMARY KEY,
        shop_name VARCHAR(100) NOT NULL,
        country VARCHAR(50),
        established_year INT
    );

    CREATE TABLE Products (
        product_id SERIAL PRIMARY KEY,
        shop_id INT,
        product_name VARCHAR(100) NOT NULL,
        price DECIMAL(8,2)
    );

    CREATE TABLE Orders (
        order_id SERIAL PRIMARY KEY,
        shop_id INT,
        order_date DATE NOT NULL,
        total_amount DECIMAL(10,2)
    );
    """

    # Parse the SQL script to extract table definitions
    tables = parse_create_tables(sql_script)

    # Create the data generator instance with the parsed schema and mappings with 95% threshold
    dg = DataGenerator(
        tables,
        num_rows=10,
        guess_column_type_mappings=True,
        threshold_for_guessing=95,
    )

    # You can also manually set column mappings if needed
    # Auto-generate column mappings using fuzzy matching just a preview
    #cmg = ColumnMappingsGenerator(threshold=80)
    #mappings = cmg.generate(tables)
    #    dg = DataGenerator(
    #    tables,
    #    num_rows=10,
    #    column_mappings=mappings,
    #)

    # printing the inferred mappings preview
    data_generator.preview_inferred_mappings()

    # Generate synthetic data and print statistics
    data = dg.generate_data()
    for table, rows in data.items():
        print(f"Table {table}:")
        for row in rows:
            print(row)

Additional Resources
--------------------

- **API Reference:**
  For a detailed API reference of each module and function, see the following pages:

  .. toctree::
     :maxdepth: 2
     :caption: Modules

     parsing
     filling
     check_constraint_evaluator
     column_mappings_generator

- **Examples and Tutorials:**
  Refer to the example usage page for step-by-step tutorials and advanced configurations.

- **GitHub Repository:**
  Visit our `GitHub repository <https://github.com/Kamil-Krawiec/Data-filler>`_ for source code, issues, and pull requests.

Indices and Tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
