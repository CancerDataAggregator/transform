from collections import defaultdict
from typing import Dict, Set, Callable, Any, Mapping, Union, List


class LogValidation:

    def __init__(self) -> None:
        # map of field name -> set of values
        self._distinct_fields: Dict[str, Set] = defaultdict(set)
        # map of ID -> (map of field name -> set of values)
        self._matching_fields: Dict[str, Dict[str, Set[str]]] = {}
        # map of field name -> invalid value
        self._invalid_fields: Dict[str, str] = {}

    def distinct(self, tables: Union[Mapping[str, Any], List[Mapping[str, Any]]],
                 field_name: str) -> None:
        """
        For every use of a field in one or more tables, track all distinct values.
        :param field_name: the field name
        :return: the value of the field in the table
        :param tables: either a table or a list of tables to find the field in
        """
        if type(tables) is not list:
            tables = [ tables ]
        for table in tables:
            value = table.get(field_name)
            if value:
                self._distinct_fields[field_name].add(value)

    def agree(self, tables: Union[Mapping[str, Any], List[Mapping[str, Any]]], record_id: str,
              fields: list) -> None:
        """
        Given an ID and a list of field names, track all values seen. For the same ID, the field
         values should match.
        :param record_id: the identifier for the record, shared across different rows
        :param fields: the fields that should match for all records
        :param tables: either a table or a list of tables tables to find the fields in
        """
        if type(tables) is not list:
            tables = [ tables ]
        for table in tables:
            other = self._matching_fields.get(record_id)
            if other:
                for field in fields:
                    value = table.get(field)
                    if value:
                        other[field].add(value)
            else:
                self._matching_fields[record_id] = {}
                for field in fields:
                    value = table.get(field)
                    if value:
                        self._matching_fields[record_id][field] = {value}

    def validate(self, tables: Union[Mapping[str, Any], List[Mapping[str, Any]]], field_name: str,
                 f: Callable[[Any], bool]) -> None:
        """
        Validate a field in one or more tables using function f. Invalid values are collected
         for reporting.
        :param field_name: the field name
        :param f: called with the field value; if true, the value is valid
        :param tables: either a table or a list of tables that contain the field
        """
        if type(tables) is not list:
            tables = [ tables ]
        for table in tables:
            value = table.get(field_name)
            if not f(value):
                self._invalid_fields[field_name] = value

    def generate_report(self, logger) -> None:

        # Note that set output is sorted for legibility and to get consistent results for testing.

        logger.info("==== Validation Report ====")

        for name, values in self._distinct_fields.items():
            if len(values) != 0:
                logger.info(f"Distinct FIELD:{name} VALUES:{':'.join(sorted(values))}")

        for id, fields in self._matching_fields.items():
            for field, values in fields.items():
                if len(values) > 1:
                    logger.warning(f"Conflict ID:{id} FIELD:{field} VALUES:{':'.join(sorted(values))}")

        for field, value in self._invalid_fields.items():
            logger.warning(f"Invalid FIELD:{field} VALUE:{value}")
