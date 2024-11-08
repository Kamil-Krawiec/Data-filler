Example of Usage
================


This section provides a practical example of how to utilize the **Intelligent Data Generator** to create and manage synthetic data for your PostgreSQL databases. The example demonstrates parsing SQL scripts, setting up data generation configurations, and exporting the generated data as SQL insert queries.

Prerequisites
-------------
- **Intelligent Data Generator** installed via `pip`:

  .. code-block:: bash

      pip install intelligent-data-generator

- **Python 3.10+** environment.

Step-by-Step Guide
------------------


1. Import Necessary Modules
***************************


Begin by importing the required functions and classes from the `parsing` and `filling` modules, along with the `pprint` module for pretty-printing dictionaries.

.. code-block:: python

    from parsing.parsing import parse_create_tables
    from filling.data_generator import DataGenerator
    import pprint # optional for pretty-printing

2. Define and Parse the SQL Script
**********************************

Provide the SQL script containing `CREATE TABLE` statements. This script defines the structure of your database, including tables, columns, data types, constraints, and foreign keys.

.. code-block:: python

    # Read and parse the SQL script
    sql_script = """
    CREATE TABLE Authors (
        author_id SERIAL PRIMARY KEY,
        sex CHAR(1) NOT NULL,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        birth_date DATE NOT NULL,

        CONSTRAINT unique_author_name UNIQUE (first_name, last_name)
    );

    CREATE TABLE Categories (
        category_id SERIAL PRIMARY KEY,
        category_name VARCHAR(50) NOT NULL UNIQUE
    );

    CREATE TABLE Books (
        book_id SERIAL PRIMARY KEY,
        title VARCHAR(100) NOT NULL,
        isbn VARCHAR(13) NOT NULL UNIQUE,
        author_id INT NOT NULL,
        publication_year INT NOT NULL,
        category_id INT NOT NULL,
        penalty_rate DECIMAL(5,2) NOT NULL,

        CONSTRAINT fk_books_author
            FOREIGN KEY(author_id)
            REFERENCES Authors(author_id),

        CONSTRAINT fk_books_category
            FOREIGN KEY(category_id)
            REFERENCES Categories(category_id),

        CONSTRAINT chk_isbn_format
            CHECK (isbn ~ '^\\d{13}$'),

        CONSTRAINT chk_publication_year
            CHECK (publication_year >= 1900 AND publication_year <= EXTRACT(YEAR FROM CURRENT_DATE))
    );

    CREATE TABLE Members (
        member_id SERIAL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) NOT NULL UNIQUE,
        registration_date DATE NOT NULL,

        CONSTRAINT chk_email_format
            CHECK (email ~ '^[\\w\\.-]+@[\\w\\.-]+\\.\\w{2,}$')
    );

    CREATE TABLE Loans (
        loan_id SERIAL PRIMARY KEY,
        book_id INT NOT NULL,
        member_id INT NOT NULL,
        loan_date DATE NOT NULL,
        due_date DATE NOT NULL,
        return_date DATE,

        CONSTRAINT fk_loans_book
            FOREIGN KEY(book_id)
            REFERENCES Books(book_id),

        CONSTRAINT fk_loans_member
            FOREIGN KEY(member_id)
            REFERENCES Members(member_id),

        CONSTRAINT chk_due_date
            CHECK (due_date > loan_date),

        CONSTRAINT chk_return_date
            CHECK (return_date IS NULL OR return_date > loan_date)
    );

    CREATE TABLE Penalties (
        penalty_id SERIAL PRIMARY KEY,
        loan_id INT NOT NULL,
        penalty_amount DECIMAL(10,2) NOT NULL,
        penalty_date DATE NOT NULL,

        CONSTRAINT fk_penalties_loan
            FOREIGN KEY(loan_id)
            REFERENCES Loans(loan_id),

        CONSTRAINT chk_penalty_amount
            CHECK (penalty_amount > 0)
    );
    """
    tables_parsed = parse_create_tables(sql_script)

3. Define Predefined Values and Column Type Mappings
*****************************************************

Set up dictionaries to define predefined values for certain columns and mappings for column types. These configurations help in generating realistic and context-aware synthetic data.
'global' values are applicable to all tables when there are several with the same column name, while table-specific values are defined under the respective table names. As we can see in the Author example, first_name is treated differently in the Authors table than in the global scope.
We use lambda functions to generate dynamic values based on the row context, such as birth_date. The `fake` parameter is a `Faker` instance that can be used to generate various types of fake data.

.. code-block:: python

    predefined_values = {
        'global': {
            'sex': ['M', 'F'],
        },
        'Categories': {
            'category_name': [
                'Fiction', 'Non-fiction', 'Science', 'History', 'Biography',
                'Fantasy', 'Mystery', 'Romance', 'Horror', 'Poetry'
            ]
        },
    }

    column_type_mappings = {
        'global': {
            'first_name': lambda fake, row: fake.first_name_male() if row.get('sex') == 'M' else fake.first_name_female(),
            'last_name': 'last_name',
            'email': 'email',
        },
        'Authors': {
            'first_name': lambda fake, row: "Author",
            'birth_date': lambda fake, row: fake.date_of_birth(minimum_age=25, maximum_age=90),
        },
        'Members': {
            'birth_date': lambda fake, row: fake.date_of_birth(minimum_age=18, maximum_age=60),
            'registration_date': lambda fake, row: fake.date_between(start_date='-5y', end_date='today')
        }
    }

4. Specify Number of Rows per Table
************************************

Define how many synthetic rows you want to generate for each table.

.. code-block:: python

    num_rows_per_table = {
        "Categories": 10,
        "Members": 20,
        "Books": 200,
        "Authors": 100,
    }

5. Initialize the Data Generator
*******************************

Create an instance of `DataGenerator` by passing the parsed tables and the configuration dictionaries defined earlier.

.. code-block:: python

    # Create an instance of DataGenerator with the parsed tables and desired number of rows
    data_generator = DataGenerator(
        tables_parsed,
        num_rows=10,
        predefined_values=predefined_values,
        column_type_mappings=column_type_mappings,
        num_rows_per_table=num_rows_per_table
    )

6. Generate Fake Data
**********************

Use the `generate_data` method to create synthetic data based on your configurations.

.. code-block:: python

    # Generate the fake data
    fake_data = data_generator.generate_data()

7. Export Generated Data as SQL Insert Queries
**********************************************

Export the generated synthetic data into SQL insert queries and save them to a `.sql` file for database population.

.. code-block:: python

    # Write SQL queries to file
    with open("DB_infos/fake_data_library.sql", "w") as f:
        f.write(data_generator.export_as_sql_insert_query())

8. Optional: Pretty-Print Generated Data
****************************************

If you wish to inspect the generated data in a readable format, you can use the `pprint` module.

.. code-block:: python

    # Pretty-print the generated data
    pprint.pprint(fake_data)

Complete Example
----------------

Putting it all together, here's the complete script:

.. code-block:: python

    from parsing.parsing import parse_create_tables
    from filling.data_generator import DataGenerator
    import pprint

    # Read and parse the SQL script
    sql_script = """
    CREATE TABLE Authors (
        author_id SERIAL PRIMARY KEY,
        sex CHAR(1) NOT NULL,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        birth_date DATE NOT NULL,

        CONSTRAINT unique_author_name UNIQUE (first_name, last_name)
    );

    CREATE TABLE Categories (
        category_id SERIAL PRIMARY KEY,
        category_name VARCHAR(50) NOT NULL UNIQUE
    );

    CREATE TABLE Books (
        book_id SERIAL PRIMARY KEY,
        title VARCHAR(100) NOT NULL,
        isbn VARCHAR(13) NOT NULL UNIQUE,
        author_id INT NOT NULL,
        publication_year INT NOT NULL,
        category_id INT NOT NULL,
        penalty_rate DECIMAL(5,2) NOT NULL,

        CONSTRAINT fk_books_author
            FOREIGN KEY(author_id)
            REFERENCES Authors(author_id),

        CONSTRAINT fk_books_category
            FOREIGN KEY(category_id)
            REFERENCES Categories(category_id),

        CONSTRAINT chk_isbn_format
            CHECK (isbn ~ '^\\d{13}$'),

        CONSTRAINT chk_publication_year
            CHECK (publication_year >= 1900 AND publication_year <= EXTRACT(YEAR FROM CURRENT_DATE))
    );

    CREATE TABLE Members (
        member_id SERIAL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) NOT NULL UNIQUE,
        registration_date DATE NOT NULL,

        CONSTRAINT chk_email_format
            CHECK (email ~ '^[\\w\\.-]+@[\\w\\.-]+\\.\\w{2,}$')
    );

    CREATE TABLE Loans (
        loan_id SERIAL PRIMARY KEY,
        book_id INT NOT NULL,
        member_id INT NOT NULL,
        loan_date DATE NOT NULL,
        due_date DATE NOT NULL,
        return_date DATE,

        CONSTRAINT fk_loans_book
            FOREIGN KEY(book_id)
            REFERENCES Books(book_id),

        CONSTRAINT fk_loans_member
            FOREIGN KEY(member_id)
            REFERENCES Members(member_id),

        CONSTRAINT chk_due_date
            CHECK (due_date > loan_date),

        CONSTRAINT chk_return_date
            CHECK (return_date IS NULL OR return_date > loan_date)
    );

    CREATE TABLE Penalties (
        penalty_id SERIAL PRIMARY KEY,
        loan_id INT NOT NULL,
        penalty_amount DECIMAL(10,2) NOT NULL,
        penalty_date DATE NOT NULL,

        CONSTRAINT fk_penalties_loan
            FOREIGN KEY(loan_id)
            REFERENCES Loans(loan_id),

        CONSTRAINT chk_penalty_amount
            CHECK (penalty_amount > 0)
    );
    """
    tables_parsed = parse_create_tables(sql_script)

    predefined_values = {
        'global': {
            'sex': ['M', 'F'],
        },
        'Categories': {
            'category_name': [
                'Fiction', 'Non-fiction', 'Science', 'History', 'Biography',
                'Fantasy', 'Mystery', 'Romance', 'Horror', 'Poetry'
            ]
        },
    }
    column_type_mappings = {
        'global': {
            'first_name': lambda fake, row: fake.first_name_male() if row.get('sex') == 'M' else fake.first_name_female(),
            'last_name': 'last_name',
            'email': 'email',
        },
        'Authors': {
            'first_name': lambda fake, row: "Author",
            'birth_date': lambda fake, row: fake.date_of_birth(minimum_age=25, maximum_age=90),
        },
        'Members': {
            'birth_date': lambda fake, row: fake.date_of_birth(minimum_age=18, maximum_age=60),
            'registration_date': lambda fake, row: fake.date_between(start_date='-5y', end_date='today')
        }
    }

    num_rows_per_table = {
        "Categories": 10,
        "Members": 20,
        "Books": 200,
        "Authors": 100,
    }

    # Create an instance of DataGenerator with the parsed tables and desired number of rows
    data_generator = DataGenerator(
        tables_parsed,
        num_rows=10,
        predefined_values=predefined_values,
        column_type_mappings=column_type_mappings,
        num_rows_per_table=num_rows_per_table
    )

    # Generate the fake data
    fake_data = data_generator.generate_data()

    # Write SQL queries to file
    with open("DB_infos/fake_data_library.sql", "w") as f:
        f.write(data_generator.export_as_sql_insert_query())

    # Optional: Pretty-print the generated data
    pprint.pprint(fake_data)