from collections import defaultdict
from typing import Dict, Set


class LogValidation:

    def __init__(self) -> None:
        # map of field name -> set of values
        self._distinct_fields: Dict[str, Set] = defaultdict(set)
        # map of ID -> (map of field name -> set of values)
        self._matching_fields: Dict[str, Dict[str, Set[str]]] = {}

    def distinct(self, table, field_name: str) -> str:
        """
        For every use of a field in a table, track all distinct values.
        :param table: the table to find the field in
        :param field_name: the field name
        :return: the value of the field in the table
        """
        value = table.get(field_name)
        self._distinct_fields[field_name].add(value)
        return value

    def agree(self, table, record_id: str, fields: list) -> None:
        """
        Given an ID and a list of field names, track all values seen. For the same ID, the field values should match.
        :param table: the table to find the fields in
        :param record_id: the identifier for the record, shared across different rows
        :param fields: the fields that should match for all records
        """
        other = self._matching_fields.get(record_id)
        if other:
            for field in fields:
                other[field].add(table.get(field))
        else:
            self._matching_fields[record_id] = {}
            for field in fields:
                self._matching_fields[record_id][field] = {table.get(field)}

    def generate_report(self, logger) -> None:

        # Note that set output is sorted for legibility and to get consistent results for testing.

        logger.info("==== Validation Report ====")
        for name, values in self._distinct_fields.items():
            if len(values) != 0:
                logger.info(f"Distinct Field :{name}: {sorted(values, key=lambda x: (x is None, x))}")

        for id, fields in self._matching_fields.items():
            for field, values in fields.items():
                if len(values) > 1:
                    logger.warning(f"Conflicting Field: ID:{id} FIELD:{field} VALUES:{':'.join(sorted(values, key=lambda x: (x is None, x)))}")
