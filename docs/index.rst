intelligent-data-generator Documentation
========================================

Welcome to the **Intelligent Data Generator** documentation! This tool is designed to automate the creation and management of synthetic data tailored to your database schemas. Whether you're developing, testing, or demonstrating your applications, our tool simplifies data generation tasks, ensuring efficiency and accuracy.


**Intelligent Data Generator** is available on `PyPI <https://pypi.org/project/intelligent-data-generator/>`_ and can be easily installed using `pip`:

.. code-block:: bash

    pip install intelligent-data-generator

Overview
--------

The Intelligent Data Generator offers a suite of modules that work seamlessly to parse database schemas, evaluate constraints, and generate realistic synthetic data. Key functionalities include:

- **Parsing Module:** Analyzes SQL scripts to extract table schemas, constraints, and relationships.
- **Filling Module:** Generates and populates synthetic data based on parsed schemas with customizable data generation strategies.
- **Constraint Evaluator:** Ensures generated data adheres to defined constraints and maintains data integrity.

Features
--------

- **Automated Schema Parsing:** Quickly interpret complex SQL scripts to understand database structures.
- **Customizable Data Generation:** Tailor data generation strategies to fit specific testing and development needs.
- **Integrity Enforcement:** Maintain data consistency and integrity by adhering to defined constraints and relationships.
- **Dependency Management:** Automatically creates dependent classes and establishes real connections between tables based on foreign keys.
- **Intelligent Value Generation:** Strives to understand the data context to generate the most appropriate and realistic values.
- **Error Handling:** Implements a repairment system that deletes all incompatible rows when it fails to generate valid data, ensuring database integrity.

Getting Started
---------------

To begin exploring the Intelligent Data Generator, refer to the detailed documentation of each module below:

.. toctree::
   :maxdepth: 2
   :caption: Modules

   parsing
   filling

Installation
------------

Install the **Intelligent Data Generator** via `pip`:

.. code-block:: bash

    pip install intelligent-data-generator

For more information, visit the `PyPI page <https://pypi.org/project/intelligent-data-generator>`_,
Or github page `Intelligent Data Generator <https://github.com/Kamil-Krawiec/Data-filler>`_.


.. toctree::
   :maxdepth: 1
   :caption: Example usage

   example_of_usage

For more detailed examples and advanced configurations, refer to the respective module documentation.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`