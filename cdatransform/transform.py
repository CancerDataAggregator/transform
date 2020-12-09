from typing import Union
import sys

import yaml
from yaml import Loader

from .transformlib import XFunc
from .gdclib import (entity_to_specimen, demographics, none_to_zero)

v_lib = {
    "none_to_zero": none_to_zero
}

s_lib = {
    "gdc.specimen": entity_to_specimen
    , "gdc.demographics": demographics
}


def parse_transforms(t_list, t_lib):
    sys.stderr.write(f"Transforms that will be applied:\n")
    _transforms = []
    for xform in t_list:
        f = parse_transform_call(xform, t_lib)
        if f is not None:
            _transforms += [f]
    return _transforms


def parse_transform_call(xform, t_lib):
    if isinstance(xform, dict):
        t_name, t_params = next(iter(xform.items()))
    else:
        t_name = xform
        t_params = {}
    
    f = t_lib.get(t_name)
    if f is not None:
        sys.stdout.write(f"{t_name}: {f.__doc__}\n")
        return XFunc(func=f, params=t_params)
    else:
        sys.stdout.write(f"{t_name}: Unknown transform!\n")
        return None


class Transform:
    def __init__(self, transform_file) -> None:
        t_list = yaml.load(open(transform_file, "r"), Loader=Loader)
        self._v_transforms = parse_transforms(t_list.get("value"), v_lib)
        self._s_transforms = parse_transforms(t_list.get("structural"), s_lib)

    def __call__(self, case: dict) -> dict:
        # Parameter passing?
        # Transform error logging?
        v_case = case
        for vt in self._v_transforms:
            v_case = vt.func(v_case, **vt.params)

        t_case = {}
        for st in self._s_transforms:
            t_case = st.func(t_case, v_case, **st.params)
        return t_case
