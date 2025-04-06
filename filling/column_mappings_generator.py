import re
import random
from datetime import date, datetime
from faker import Faker
from fuzzywuzzy import fuzz, process


class ColumnMappingsGenerator:
    """
    Attempts to auto-generate column_type_mappings by:
      - Checking for ENUM(...) in col_type,
      - Checking for 'IN' constraint sets,
      - Handling numeric bounds from check constraints,
      - Fuzzy-matching column names against Faker for everything else.
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
        For each table & column in schema, produce a mapping function
        that yields data in compliance with (or close to) col_type and constraints.
        """
        mappings = {}
        for table_name, table_info in schema.items():
            col_map = {}
            columns = table_info.get('columns', [])
            for col_def in columns:
                col_name = col_def['name']
                col_type = col_def.get('type', '').upper()
                constraints = col_def.get('constraints', [])

                # 1) Try to parse an enum set from the column type
                enum_vals = self._extract_enum_values(col_type)

                # 2) Also parse if there's an IN(...) constraint
                in_vals = self._extract_in_constraint_values(constraints, col_name)

                # unify sets if both found
                if enum_vals and in_vals:
                    # union the two sets
                    final_vals = list(set(enum_vals) | set(in_vals))
                elif enum_vals:
                    final_vals = enum_vals
                elif in_vals:
                    final_vals = in_vals
                else:
                    final_vals = None  # no enumerated set

                # If we have enumerated values, skip fuzzy logic & produce random choice generator
                if final_vals:
                    col_map[col_name] = self._make_enum_in_generator(final_vals, col_type)
                    continue

                # If we see 'SERIAL', produce numeric
                if 'SERIAL' in col_type:
                    col_map[col_name] = self._serial_generator()
                    continue

                # 3) Attempt numeric range parse
                min_val, max_val = self._extract_numeric_bounds(constraints, col_name)

                # 4) Fuzzy guess a Faker method
                guess_method = self._fuzzy_guess_faker_method(col_name)
                if guess_method is not None:
                    col_map[col_name] = self._wrap_faker_call(guess_method, col_type, min_val, max_val)
                else:
                    # fallback
                    col_map[col_name] = self._fallback_generator(col_type, min_val, max_val)

            if col_map:
                mappings[table_name] = col_map
        return mappings

    # --------------------------------------------------------------------------
    # Internals
    # --------------------------------------------------------------------------

    def _gather_faker_methods(self):
        """
        Return a list of all publicly callable faker methods,
        skipping 'seed' and other known special methods.
        """
        all_attrs = dir(self.fake)
        methods = []
        for attr in all_attrs:
            if attr.startswith('_'):
                continue
            if attr in ('seed', 'seed_instance'):
                continue
            try:
                candidate = getattr(self.fake, attr, None)
                if callable(candidate):
                    methods.append(attr)
            except TypeError:
                continue
        return methods

    def _fuzzy_guess_faker_method(self, col_name: str):
        """
        Use fuzzy matching to pick the best faker method from 'self.faker_methods'.
        Return method name (like 'email') or None if no match is good enough.
        """
        if not self.faker_methods:
            return None

        best_match, score = process.extractOne(
            col_name, self.faker_methods, scorer=fuzz.WRatio
        )
        if score >= self.threshold:
            return best_match
        return None

    def _wrap_faker_call(self, method_name: str, col_type: str,
                         min_val: float or None, max_val: float or None):
        """
        Return a lambda that calls `fake.method_name()`,
        then tries to cast/fix the result for 'col_type',
        respecting numeric bounds if any.
        """

        def generator(fake: Faker, row: dict):
            val = getattr(fake, method_name)()

            # numeric?
            if any(t in col_type for t in ['INT','BIGINT','SMALLINT','DECIMAL','NUMERIC','FLOAT']):
                return self._coerce_numeric(val, col_type, min_val, max_val, fallback=fake)
            elif 'DATE' in col_type:
                return self._coerce_date(val, fake)

            # else text
            if not isinstance(val, str):
                val = str(val)
            length_match = re.search(r'\((\d+)\)', col_type)
            if length_match:
                max_len = int(length_match.group(1))
                val = val[:max_len]
            return val

        return generator

    def _fallback_generator(self, col_type: str, min_val, max_val):
        """
        If no method matched, produce default generator for numeric, date or text,
        respecting [min_val, max_val] if numeric.
        """
        def fallback(fake: Faker, row: dict):
            if any(t in col_type for t in ['INT','BIGINT','SMALLINT','DECIMAL','NUMERIC','FLOAT']):
                return self._coerce_numeric(None, col_type, min_val, max_val, fallback=fake)
            elif 'DATE' in col_type:
                return fake.date_between(start_date='-30y', end_date='today')
            else:
                return fake.word()
        return fallback

    def _serial_generator(self):
        """
        Return a lambda that yields numeric value for 'SERIAL' columns.
        """
        def gen_serial(fake: Faker, row: dict):
            return fake.random_int(min=1, max=999999)
        return gen_serial

    # -- new methods for enumerated sets:

    def _extract_enum_values(self, col_type: str):
        """
        If col_type is like ENUM('M','F','OTHER'), parse out ['M','F','OTHER'].
        Return list of possible values or None if not found.
        """
        if not col_type.startswith('ENUM('):
            return None
        # e.g. col_type = \"ENUM('M','F','OTHER')\"
        match = re.match(r"^ENUM\((.*)\)$", col_type)
        if not match:
            return None
        inside = match.group(1)  # 'M','F','OTHER'
        # parse splitted, e.g. " 'M','F','OTHER' "
        # a simple approach is to split by comma, strip quotes
        # careful with whitespace
        raw_vals = [v.strip() for v in inside.split(',')]
        enum_vals = []
        for rv in raw_vals:
            # strip leading/trailing quotes
            rv_clean = rv.strip().strip("'").strip('"')
            if rv_clean:
                enum_vals.append(rv_clean)
        return enum_vals if enum_vals else None

    def _extract_in_constraint_values(self, constraints, col_name: str):
        """
        If we find a constraint like:
          CHECK ( col_name IN ('val1','val2','val3') )
        parse out the possible values [ 'val1', 'val2', 'val3'].
        Return list or None if not found.
        """
        in_vals = None
        for c in constraints:
            if 'CHECK' not in c.upper():
                continue
            # e.g. \"CHECK ( rating IN (1,2,3,4,5) )\" or
            #     \"CHECK ( sex IN ('M','F') )\"
            # We'll do a naive pattern search for col_name IN ( ...)
            pat = rf"{col_name}\s+IN\s*\((.*?)\)"
            match = re.search(pat, c, flags=re.IGNORECASE)
            if match:
                # parse the inside
                inside = match.group(1)  # e.g. \"'M','F'\" or \"1,2,3\"
                # split by comma
                raw_vals = [x.strip() for x in inside.split(',')]
                results = []
                for rv in raw_vals:
                    # strip quotes if present
                    val = rv.strip("'\" ")
                    if val:
                        results.append(val)
                # if numeric, we can keep them numeric. But typically these are strings.
                if results:
                    in_vals = results
                # We only handle the first match, or we can break or keep searching
        return in_vals

    def _make_enum_in_generator(self, possible_vals, col_type):
        """
        Return a lambda that picks from 'possible_vals' at random,
        optionally respecting length limits if text-based,
        or converting numeric if col_type is numeric.
        """
        def gen_enum_in(fake: Faker, row: dict):
            val = random.choice(possible_vals)
            # if col_type is numeric, parse
            if any(t in col_type for t in ['INT','BIGINT','SMALLINT','DECIMAL','NUMERIC','FLOAT']):
                try:
                    flt = float(val)
                    # if int-based
                    if any(t in col_type for t in ['INT','BIGINT','SMALLINT']):
                        return int(flt)
                    return flt
                except ValueError:
                    # fallback random numeric
                    return fake.random_int(min=0, max=9999)
            elif 'DATE' in col_type:
                # try parse as date
                try:
                    dt = datetime.strptime(val, '%Y-%m-%d')
                    return dt.date()
                except ValueError:
                    return fake.date_between(start_date='-30y', end_date='today')
            else:
                # text-based
                length_match = re.search(r'\((\d+)\)', col_type)
                if length_match:
                    max_len = int(length_match.group(1))
                    return val[:max_len]
                return val
        return gen_enum_in

    # -- reusing your numeric range and date coercion from prior code:

    def _extract_numeric_bounds(self, constraints, col_name: str):
        """
        Attempt to parse something like:
          'CHECK (rating >= 1 AND rating <= 5)'
        to glean (1, 5). Return (None, None) if no simple match found.
        """
        min_val, max_val = None, None
        for c in constraints:
            if 'CHECK' not in c.upper():
                continue
            pat_ge = rf'{col_name}\s*>=\s*(\d+(?:\.\d+)?)'
            pat_le = rf'{col_name}\s*<=\s*(\d+(?:\.\d+)?)'
            match_ge = re.search(pat_ge, c, re.IGNORECASE)
            match_le = re.search(pat_le, c, re.IGNORECASE)
            if match_ge:
                try:
                    min_val = float(match_ge.group(1))
                except ValueError:
                    pass
            if match_le:
                try:
                    max_val = float(match_le.group(1))
                except ValueError:
                    pass
        return (min_val, max_val)

    def _coerce_numeric(self, val, col_type, min_val, max_val, fallback=None):
        """
        Coerce val into a numeric within [min_val, max_val], or fallback if parse fails.
        """
        local_min = min_val if (min_val is not None) else 0
        local_max = max_val if (max_val is not None) else 9999

        if fallback is None:
            fallback_fake = self.fake
        else:
            fallback_fake = fallback

        if val is not None:
            try:
                flt = float(val)
                if any(t in col_type for t in ['INT','BIGINT','SMALLINT']):
                    flt = int(flt)
                if flt < local_min:
                    flt = local_min
                if flt > local_max:
                    flt = local_max
                return flt
            except (ValueError, TypeError):
                pass

        # fallback
        if any(t in col_type for t in ['INT','BIGINT','SMALLINT']):
            return fallback_fake.random_int(min=int(local_min), max=int(local_max))
        else:
            return random.uniform(local_min, local_max)

    def _coerce_date(self, val, fake: Faker):
        """
        Ensure 'val' is returned as a date. If parsing fails, fallback to random date.
        """
        if isinstance(val, date):
            return val
        if isinstance(val, datetime):
            return val.date()
        if isinstance(val, str):
            try:
                dt = datetime.strptime(val, '%Y-%m-%d')
                return dt.date()
            except ValueError:
                pass
        # fallback
        return fake.date_between(start_date='-30y', end_date='today')