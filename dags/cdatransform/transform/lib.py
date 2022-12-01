import logging
import os

import yaml
from yaml import Loader

try:
    import cdatransform.transform.transform_lib.transform_with_YAML_v1 as tr
    from cdatransform.transform.gdclib import diagnosis as gdclib_diagnosis
    from cdatransform.transform.gdclib import (
        entity_to_specimen as gdclib_entity_to_specimen,
    )
    from cdatransform.transform.gdclib import patient as gdclib_patient
    from cdatransform.transform.gdclib import (
        research_subject as gdclib_research_subject,
    )
    from cdatransform.transform.pdclib import diagnosis as pdclib_diagnosis
    from cdatransform.transform.pdclib import (
        entity_to_specimen as pdclib_entity_to_specimen,
    )
    from cdatransform.transform.pdclib import patient as pdclib_patient
    from cdatransform.transform.pdclib import (
        research_subject as pdclib_research_subject,
    )
except ImportError:
    import dags.cdatransform.transform.transform_lib.transform_with_YAML_v1 as tr
    from dags.cdatransform.transform.gdclib import diagnosis as gdclib_diagnosis
    from dags.cdatransform.transform.gdclib import (
        entity_to_specimen as gdclib_entity_to_specimen,
    )
    from dags.cdatransform.transform.gdclib import patient as gdclib_patient
    from dags.cdatransform.transform.gdclib import (
        research_subject as gdclib_research_subject,
    )
    from dags.cdatransform.transform.pdclib import diagnosis as pdclib_diagnosis
    from dags.cdatransform.transform.pdclib import (
        entity_to_specimen as pdclib_entity_to_specimen,
    )
    from dags.cdatransform.transform.pdclib import patient as pdclib_patient
    from dags.cdatransform.transform.pdclib import (
        research_subject as pdclib_research_subject,
    )

logger = logging.getLogger(__name__)

t_lib = {
    "gdc.research_subject": gdclib_research_subject,
    "gdc.diagnosis": gdclib_diagnosis,
    "gdc.entity_to_specimen": gdclib_entity_to_specimen,
    "gdc.patient": gdclib_patient,
    "pdc.diagnosis": pdclib_diagnosis,
    "pdc.entity_to_specimen": pdclib_entity_to_specimen,
    "pdc.patient": pdclib_patient,
    "pdc.research_subject": pdclib_research_subject,
}


def get_transformation_mapping(file_name: str):
    if "MERGE_AND_MAPPING_DIRECTORY" in os.environ:
        mapping_dir = os.environ["MERGE_AND_MAPPING_DIRECTORY"]
    else:
        mapping_dir = "./dags/yaml_merge_and_mapping_dir"

    YAMLFILEDIR = f"{mapping_dir}/mapping/"
    if file_name is None or len(file_name) == 0:
        yaml_mapping_transform_file = f"{YAMLFILEDIR}GDC_subject_endpoint_mapping.yml"
    else:
        yaml_mapping_transform_file = f"{YAMLFILEDIR}{file_name}"
    # endpoint = ["case","file",]
    # this is a str for the endpoints will change use param later

    mapping_and_transformation: dict[str, dict[str, dict]] = yaml.load(
        open(yaml_mapping_transform_file, "r"), Loader=Loader
    )

    for entity, mapping_or_transformation_dict in mapping_and_transformation.items():
        if "Transformations" in mapping_or_transformation_dict:
            mapping_and_transformation[entity][
                "Transformations"
            ] = tr.functionalize_trans_dict(
                mapping_and_transformation[entity]["Transformations"]
            )

    return mapping_and_transformation


def parse_transforms(t_list, t_lib):
    logger.info("Loading transforms")

    _transforms = []
    if not isinstance(t_list, list):
        logger.error("Transforms must be a list")
        return _transforms

    for n, xform in enumerate(t_list):
        _f = ""
        _p = {}

        if isinstance(xform, str):
            _f, _p = xform, {}
        elif isinstance(xform, dict):
            if len(xform.keys()) != 1:
                logger.error(f"Transform #{n} is ill-formed")
                continue
            for _k, _v in xform.items():
                _f, _p = _k, _v
        else:
            logger.error(f"Transform #{n} is ill-formed")
            continue

        if _f not in t_lib:
            logger.error(f"unknown transform '{_f}'")
            continue

        _func = t_lib[_f]
        logger.info(f"Added transform {_f}: {_func.__doc__}")
        _transforms += [(_func, _p)]

    if len(_transforms) != len(t_list):
        _transforms = []
        logger.error("Will not run while there are issues with transforms.")
        raise RuntimeError("Transforms have errors, please check log file.")

    return _transforms


class Transform:
    def __init__(self, t_list, validate) -> None:
        self._transforms = parse_transforms(t_list, t_lib)
        self._validate = validate

    def __call__(self, source: dict) -> dict:
        destination = {}
        for vt in self._transforms:
            destination = vt[0](destination, source, self._validate, **vt[1])
        return destination
