Column Mappings Generator Documentation
=========================================

Overview
--------
The ``ColumnMappingsGenerator`` module provides functionality for automatically generating column mapping functions based on a given SQL schema. It analyzes column types and constraints to choose appropriate data generators by:

- Parsing ENUM definitions and IN(...) constraints.
- Extracting numeric bounds from CHECK constraints.
- Using fuzzy matching to select relevant methods from the Faker library for generating realistic data.
- Providing fallback generators if no suitable match is found.

Class: ColumnMappingsGenerator
-------------------------------
The class is designed to produce mapping functions for each column in your schema. These mappings ensure that the generated synthetic data complies with (or approximates) the specified column types and constraints.

Key Methods:
- **``__init__(threshold=80)``**
  Initializes the generator with a fuzzy matching threshold that determines the minimum acceptable score for selecting a Faker method.

- **``generate(schema: dict) -> dict``**
  Iterates over the provided schema (a dictionary of tables and columns) and produces a mapping function for each column.

- **``_gather_faker_methods()``**
  Gathers all callable, public Faker methods (excluding special methods like ``seed`` and ``seed_instance``) to be used for fuzzy matching.

- **``_fuzzy_guess_faker_method(col_name: str)``**
  Uses fuzzy string matching (with fuzzywuzzy’s ``WRatio`` scorer) to find the best matching Faker method for a given column name.

- **``_wrap_faker_call(method_name: str, col_type: str, min_val: float or None, max_val: float or None)``**
  Wraps a Faker call in a lambda that adjusts the output (e.g., enforces numeric bounds or truncates text based on column type).

- **``_fallback_generator(col_type: str, min_val, max_val)``**
  Provides a generic data generator when no suitable Faker method is found.

- **``_serial_generator()``**
  Generates a random integer value for columns defined as ``SERIAL``.

- **``_extract_enum_values(col_type: str)``**
  Parses ENUM definitions (e.g., ``ENUM('M','F','OTHER')``) to extract possible values.

- **``_extract_in_constraint_values(constraints, col_name: str)``**
  Searches the column's constraints for an IN(...) clause and extracts the possible values.

- **``_make_enum_in_generator(possible_vals, col_type)``**
  Returns a generator function that randomly selects from the enumerated values, applying type conversions as needed.

- **``_extract_numeric_bounds(constraints, col_name: str)``**
  Attempts to extract numeric bounds from CHECK constraints (e.g., ``rating >= 1 AND rating <= 5``).

- **``_coerce_numeric(val, col_type, min_val, max_val, fallback=None)``**
  Converts a given value to a numeric type, respecting any bounds; if conversion fails, a fallback value is generated.

- **``_coerce_date(val, fake: Faker)``**
  Ensures that the value is returned as a valid date, with a fallback to a random date if conversion is unsuccessful.

Usage Example
-------------
Below is an example of how you might use the ``ColumnMappingsGenerator``:

.. code-block:: python

    from filling import ColumnMappingsGenerator

    # Initialize with a fuzzy matching threshold
    cmg = ColumnMappingsGenerator(threshold=80)

    # Define a simple schema
    schema = {
        'users': {
            'columns': [
                {'name': 'email', 'type': 'VARCHAR(255)', 'constraints': []},
                {'name': 'status', 'type': "ENUM('active','inactive')", 'constraints': []},
            ]
        }
    }

    # Generate mappings for the schema
    mappings = cmg.generate(schema)
    print(mappings)

Advanced Details
-----------------
The generator employs several advanced techniques:

1. **Enumeration Handling:**
   It extracts possible values from both ENUM definitions and IN constraints, combining them if both are present.

2. **Numeric Constraint Parsing:**
   Numeric bounds are identified from CHECK constraints to ensure generated numbers stay within valid limits.

3. **Fuzzy Matching with Faker:**
   Using the fuzzywuzzy library, the module compares column names with available Faker methods to determine the best match.

4. **Fallback Mechanism:**
   If no suitable Faker method can be identified, a generic fallback generator provides a sensible default.

Customization
-------------
- Adjust the **threshold** in the constructor to control sensitivity in selecting Faker methods.
- Extend or override mapping functions by subclassing ``ColumnMappingsGenerator``.
- Combine custom mappings (as illustrated in other guides) with the auto-generated mappings for even more control over your data.

Conclusion
----------
The ``ColumnMappingsGenerator`` is a powerful tool for automating the creation of synthetic data mappings. Its combination of constraint parsing and fuzzy matching enables realistic data generation tailored to your specific schema requirements.

For further customization or troubleshooting, refer to the additional guides included in the Intelligent Data Generator documentation.