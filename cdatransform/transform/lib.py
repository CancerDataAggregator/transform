import logging

import yaml
from yaml import Loader
import cdatransform.transform.gdclib as gdclib
import cdatransform.transform.pdclib as pdclib

logger = logging.getLogger(__name__)

t_lib = {
    "gdc.research_subject": gdclib.research_subject,
    "gdc.diagnosis": gdclib.diagnosis,
    "gdc.entity_to_specimen": gdclib.entity_to_specimen,
    "gdc.files": gdclib.add_files,
    "pdc.diagnosis": pdclib.diagnosis,
    "pdc.entity_to_specimen": pdclib.entity_to_specimen,
    "pdc.files": pdclib.add_files,
    "pdc.patient":pdclib.patient,
    "pdc.research_subject": pdclib.research_subject
}


def parse_transforms(t_list, t_lib):
    logger.info(f"Loading transforms")

    _transforms = []
    if not isinstance(t_list, list):
        logger.error("Transforms must be a list")
        return _transforms
    
    for n, xform in enumerate(t_list):
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

    return _transforms


class Transform:
    def __init__(self, transform_file) -> None:
        t_list = yaml.load(open(transform_file, "r"), Loader=Loader)
        self._transforms = parse_transforms(t_list, t_lib)

    def __call__(self, source: dict) -> dict:
        destination = {}
        for vt in self._transforms:
            destination = vt[0](destination, source, **vt[1])
        return destination
