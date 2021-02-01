import logging
from collections import defaultdict


class LogValidation:
    logger = logging.getLogger(__name__)

    def __init__(self) -> None:
        self._distinct_fields = defaultdict(set)
        self._matching_fields = {}

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
            for index, field in enumerate(fields):
                if table.get(field) == other[index]:
                    self.logger.warning(f"Conflicting Field ID :{record_id}:{field}:{table.get(field)}:{other[index]}")
        else:
            self._matching_fields[record_id] = [table.get(field) for field in fields]

    def generate_report(self) -> None:
        self.logger.info("==== Validation Report ====")
        for name, values in self._distinct_fields.items():
            if len(values) != 0:
                self.logger.info(f"Distinct Field :{name}: {sorted(values, key=lambda x: (x is None, x))}")
