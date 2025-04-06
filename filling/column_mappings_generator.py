import re
import random
from datetime import date, datetime
from faker import Faker
from fuzzywuzzy import fuzz, process


class ColumnMappingsGenerator:
    """
    Attempts to auto-generate column_type_mappings by fuzzy-matching column names
    against the public Faker methods (like 'name', 'email', 'phone_number', etc.),
    then checks or casts the return value to fit the column's SQL type (INT, DATE, etc.).
    """

    def __init__(self, threshold=80):
        """
        Args:
            threshold (int): Minimum fuzzywuzzy score to accept a match. Adjust to taste.
        """
        self.fake = Faker()
        self.threshold = threshold
        self.faker_methods = self._gather_faker_methods()

    def generate(self, schema: dict) -> dict:
        """
        Given a schema like:
            {
              'Authors': {
                'columns': [
                  {'name': 'sex',  'type': 'VARCHAR(10)', 'constraints': [...]},
                  {'name': 'age',  'type': 'INT',         'constraints': [...]},
                  ...
                ]
              },
              'Books': { ... },
              ...
            }

        Return a dictionary that looks like:
            {
              'Authors': {
                'sex':   <lambda fake, row: ...>,
                'age':   <lambda fake, row: ...>,
              },
              'Books': {...},
              ...
            }
        """
        mappings = {}
        for table_name, table_info in schema.items():
            col_map = {}
            columns = table_info.get('columns', [])
            for col_def in columns:
                col_name = col_def['name']
                col_type = col_def.get('type', '').upper()

                # Fuzzy guess a suitable Faker method for this column name:
                guess_method = self._fuzzy_guess_faker_method(col_name)

                # Wrap the guessed method call with a function that checks/casts if needed
                if guess_method is not None:
                    col_map[col_name] = self._wrap_faker_call(guess_method, col_type)
                else:
                    # If no method matched, fallback with a default generator:
                    col_map[col_name] = self._fallback_generator(col_type)

            if col_map:
                mappings[table_name] = col_map
        return mappings

    # --------------------------------------------------------------------------
    # Internals
    # --------------------------------------------------------------------------

    def _gather_faker_methods(self):
        """
        Return a list of all publicly callable faker methods,
        skipping 'seed' and any other known special methods.
        """
        all_attrs = dir(self.fake)
        methods = []
        for attr in all_attrs:
            if attr.startswith('_'):
                continue
            # skip seed methods
            if attr in ('seed', 'seed_instance'):
                continue
            try:
                candidate = getattr(self.fake, attr, None)
                if callable(candidate):
                    methods.append(attr)
            except TypeError:
                # some attributes might raise errors, skip them
                continue
        return methods

    def _fuzzy_guess_faker_method(self, col_name: str):
        """
        Use fuzzy matching to pick the best faker method from 'self.faker_methods'.
        Returns the method name string (like 'email') or None if no match is good enough.
        """
        if not self.faker_methods:
            return None

        best_match, score = process.extractOne(
            col_name, self.faker_methods, scorer=fuzz.WRatio
        )
        if score >= self.threshold:
            return best_match
        return None

    def _wrap_faker_call(self, method_name: str, col_type: str):
        """
        Return a lambda that calls `fake.method_name()`,
        then checks/casts the return value to match 'col_type'.

        For example, if col_type indicates INT, we try to parse int.
        If col_type is DATE, we try to ensure a date object, etc.
        """

        def generator(fake: Faker, row: dict):
            val = getattr(fake, method_name)()

            # Based on col_type, cast or fix 'val'
            if any(t in col_type for t in ['INT', 'BIGINT', 'SMALLINT', 'DECIMAL', 'NUMERIC']):
                # Attempt to parse as float then cast if we see 'INT'
                try:
                    flt = float(val)
                    if any(t in col_type for t in ['INT', 'BIGINT', 'SMALLINT']):
                        return int(flt)
                    else:
                        return flt  # e.g. DECIMAL -> float
                except (ValueError, TypeError):
                    # fallback if parse fails
                    return fake.random_int(min=0, max=9999)

            elif 'DATE' in col_type:
                # If val is date/datetime, return it. If it's string, parse if possible.
                if isinstance(val, date):
                    return val
                if isinstance(val, datetime):
                    return val.date()
                # Attempt parse if it's a string
                if isinstance(val, str):
                    try:
                        # parse with strptime
                        dt = datetime.strptime(val, '%Y-%m-%d')
                        return dt.date()
                    except ValueError:
                        pass
                # fallback if we can't parse
                return fake.date_between(start_date='-30y', end_date='today')

            # Otherwise assume text-ish
            # If it's not a string, convert to str
            if not isinstance(val, str):
                val = str(val)
            # If col_type has a length limit, maybe we cut it here, e.g. for CHAR(50)
            length_match = re.search(r'\((\d+)\)', col_type)
            if length_match:
                max_len = int(length_match.group(1))
                val = val[:max_len]
            return val

        return generator

    def _fallback_generator(self, col_type: str):
        """
        If no method matched or we're unsure, produce a default generator
        that yields numeric or date or string based on col_type.
        """

        def fallback(fake: Faker, row: dict):
            if any(t in col_type for t in ['INT', 'BIGINT', 'SMALLINT', 'DECIMAL', 'NUMERIC']):
                return fake.random_int(min=0, max=9999)
            elif 'DATE' in col_type:
                return fake.date_between(start_date='-30y', end_date='today')
            else:
                return fake.word()

        return fallback
