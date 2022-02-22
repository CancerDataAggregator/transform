import json

import pytest
import yaml
from yaml import Loader
from deepdiff import DeepDiff
from deepdiff.helper import CannotCompare

# from cdatransform.transform.lib import Transform
import cdatransform.transform.transform_lib.transform_with_YAML_v1 as tr
from cdatransform.transform.validate import LogValidation

# DeepDiff strictly ignoring order is now confusing two specimens.
# Add this function to look at entities with the same id
def compare_func(x, y, level=None):
    try:
        return x['id'] == y['id']
    except Exception:
        raise CannotCompare() from None
@pytest.mark.parametrize(
    "transform,DC,case,expected,endpoint",
    [
        pytest.param(
            "../GDC_subject_endpt_mapping.yml",
            "GDC",
            "steps/gdc_TARGET_case1.json",
            "steps/gdc_TARGET_case1_harmonized.yaml",
            "cases",
        ),
        pytest.param(
            "../GDC_subject_endpt_mapping.yml",
            "GDC",
            "steps/gdc_TARGET_case2.json",
            "steps/gdc_TARGET_case2_harmonized.yaml",
            "cases",
        ),
        pytest.param(
            "../GDC_file_endpt_mapping.yml",
            "GDC",
            "steps/gdc_TARGET_file1.json",
            "steps/gdc_TARGET_file1_harmonized.yaml",
            "files",
        ),
        pytest.param(
            "../GDC_file_endpt_mapping.yml",
            "GDC",
            "steps/gdc_TARGET_file2.json",
            "steps/gdc_TARGET_file2_harmonized.yaml",
            "files",
        ),
    ],
)

def test_transform(transform, DC, case, expected, endpoint):
    validate = LogValidation()
    # t_list = yaml.safe_load(open(transform, "r"))
    MandT = yaml.load(open(transform, "r"), Loader=Loader)
    for entity, MorT_dict in MandT.items():
        if "Transformations" in MorT_dict:
            MandT[entity]["Transformations"] = tr.functionalize_trans_dict(
                MandT[entity]["Transformations"]
            )
    transform = tr.Transform(validate)
    # transform = Transform(t_list, validate)
    with open(case) as case_data:
        transformed = transform(json.load(case_data), MandT, DC, endpoint=endpoint)
        with open(expected) as expected_data:
            diff = DeepDiff(
                yaml.safe_load(expected_data), transformed, ignore_order=True, iterable_compare_func=compare_func)# cutoff_distance_for_pairs=1
            #)
            if diff != {}:
                print("difference found")
                print(diff.pretty())
                outfilename = case.split('.').insert(-1,'testH')
                outfilename = '.'.join(outfilename)
                with open(outfilename, "w") as outfile:
                    json.dump(transformed, outfile)
                assert False, "output didn't match expected"
                

