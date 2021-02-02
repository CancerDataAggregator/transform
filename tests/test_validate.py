import logging
from unittest import TestCase

from cdatransform.transform.validate import LogValidation


class TestLogValidation(TestCase):

    def test_distinct(self):
        log = LogValidation()
        t1 = {"field": "val1", "field2": "val3"}
        t2 = {"field": "val2"}
        t3 = {"field2": "val3"}
        log.distinct(t1, "field")
        log.distinct(t2, "field")
        log.distinct(t3, "field")
        log.distinct(t1, "field2")
        log.distinct(t2, "field2")
        log.distinct(t3, "field2")

        with self.assertLogs('test', level='INFO') as cm:
            log.generate_report(logging.getLogger('test'))
        self.assertEqual(cm.output, ['INFO:test:==== Validation Report ====',
                                     "INFO:test:Distinct Field :field: ['val1', 'val2', None]",
                                     "INFO:test:Distinct Field :field2: ['val3', None]"])

    def test_agree(self):
        log = LogValidation()
        t1 = {"field1": "val1", "field2": "val3"}
        t2 = {"field1": "val2", "field2": "val4"}
        t3 = {"field1": "val1", "field2": "val5"}
        log.agree(t1, "id1", ["field1", "field2"])
        log.agree(t2, "id2", ["field1", "field2"])
        log.agree(t3, "id1", ["field1", "field2"])

        with self.assertLogs('test', level='INFO') as cm:
            log.generate_report(logging.getLogger('test'))
        self.assertEqual(cm.output, ['INFO:test:==== Validation Report ====',
                                     'WARNING:test:Conflicting Field: ID:id1 FIELD:field2 VALUES:val3:val5'])
