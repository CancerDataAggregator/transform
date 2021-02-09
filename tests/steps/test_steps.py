import json
from unittest import TestCase

import yaml

from cdatransform.transform.lib import Transform
from cdatransform.transform.validate import LogValidation

harmonize_test_cases = \
    {
        "gdc_TARGET_case1.json": "gdc_TARGET_case1_harmonized.yaml",
        "gdc_TARGET_case2.json": "gdc_TARGET_case2_harmonized.yaml"
     }


def test_harmonize():
    validate = LogValidation()
    transform = Transform("../../gdc-transform.yml", validate)
    for case, expected in harmonize_test_cases.items():
        with open(case) as case_data:
            transformed = transform(json.load(case_data))
            with open(expected) as expected_data:
                # print(yaml.dump(transformed))
                TestCase().assertDictEqual(transformed, yaml.load(expected_data))
