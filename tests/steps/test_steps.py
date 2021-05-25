import json

import pytest
import yaml
from yaml import Loader
from deepdiff import DeepDiff

#from cdatransform.transform.lib import Transform
import cdatransform.transform.transform_lib.transform_with_YAML_v1 as tr
from cdatransform.transform.validate import LogValidation


@pytest.mark.parametrize(
    "transform,case,expected",
    [
        pytest.param(
            "../GDC_mapping.yml", "GDC", "steps/gdc_TARGET_case1.json", "steps/gdc_TARGET_case1_harmonized.yaml"
        ),
        pytest.param(
            "../GDC_mapping.yml", "GDC", "steps/gdc_TARGET_case2.json", "steps/gdc_TARGET_case2_harmonized.yaml"
        ),
    ],
)
def test_transform(transform, DC, case, expected):
    validate = LogValidation()
    #t_list = yaml.safe_load(open(transform, "r"))
    MandT = yaml.load(open(transform,"r"), Loader=Loader)
    for entity,MorT_dict in MandT.items():
        if 'Transformations' in MorT_dict:
            MandT[entity]['Transformations'] = tr.functionalize_trans_dict(MandT[entity]['Transformations'])
    transform = tr.Transform(validate)
    #transform = Transform(t_list, validate)
    with open(case) as case_data:
        transformed = transform(json.load(case_data),MandT,DC)
        with open(expected) as expected_data:
            diff = DeepDiff(yaml.safe_load(expected_data), transformed, ignore_order=False)
            if diff != {}:
                print(diff.pretty())
                assert False, "output didn't match expected"
