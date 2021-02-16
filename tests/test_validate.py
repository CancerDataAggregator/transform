import logging
from unittest import TestCase

from cdatransform.transform.validate import LogValidation


class TestLogValidation(TestCase):

    def test_distinct(self):
        log = LogValidation()
        t1 = {"field": "val1", "field2": "val3"}
        t2 = {"field": "val2"}
        t3 = {"field2": "val3"}
        log.distinct("field", t1, t2, t3)
        log.distinct("field2", t1, t2, t3)

        with self.assertLogs('test', level='INFO') as cm:
            log.generate_report(logging.getLogger('test'))
        self.assertEqual(cm.output, ['INFO:test:==== Validation Report ====',
                                     'INFO:test:Distinct FIELD:field VALUES:val1:val2',
                                     'INFO:test:Distinct FIELD:field2 VALUES:val3'])

    def test_agree(self):
        log = LogValidation()
        t1 = {"field1": "val1", "field2": "val3"}
        t2 = {"field1": "val2", "field2": "val4"}
        t3 = {"field1": "val1", "field2": "val5"}
        log.agree("id1", ["field1", "field2"], t1, t3)
        log.agree("id2", ["field1", "field2"], t2)

        with self.assertLogs('test', level='INFO') as cm:
            log.generate_report(logging.getLogger('test'))
        self.assertEqual(cm.output, ['INFO:test:==== Validation Report ====',
                                     'WARNING:test:Conflict ID:id1 FIELD:field2 VALUES:val3:val5'])

    def test_validate(self):
        log = LogValidation()
        t1 = {"field1": 123}
        t2 = {"field1": 1234}

        def is_even(x):
            return x % 2 == 0

        log.validate("field1", is_even, t1, t2)

        with self.assertLogs('test', level='INFO') as cm:
            log.generate_report(logging.getLogger('test'))
        self.assertEqual(cm.output, ['INFO:test:==== Validation Report ====',
                                     'WARNING:test:Invalid FIELD:field1 VALUE:123'])
