import json

import pytest
import yaml
from deepdiff import DeepDiff

from cdatransform.transform.lib import Transform
from cdatransform.transform.validate import LogValidation


@pytest.mark.parametrize(
    "transform,case,expected",
    [
        pytest.param(
            "../gdc-transform.yml", "steps/gdc_TARGET_case1.json", "steps/gdc_TARGET_case1_harmonized.yaml"
        ),
        pytest.param(
            "../gdc-transform.yml", "steps/gdc_TARGET_case2.json", "steps/gdc_TARGET_case2_harmonized.yaml"
        ),
    ],
)
#@pytest.mark.xfail
def test_transform(transform, case, expected):
    validate = LogValidation()
    t_list = yaml.safe_load(open(transform, "r"))
    transform = Transform(t_list, validate)
    with open(case) as case_data:
        transformed = transform(json.load(case_data))
        with open(expected) as expected_data:
            diff = DeepDiff(yaml.safe_load(expected_data), transformed, ignore_order=True)
            if diff != {}:
                print(diff.pretty())
                assert False, "output didn't match expected"
