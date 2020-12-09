from typing import Union
import sys

import yaml
from yaml import Loader

from .gdclib import (entity_to_specimen, )

t_lib = {
    "gdc.specimen": entity_to_specimen
}


class Transform:
    def __init__(self, transform_file) -> None:
        t_list = yaml.load(open(transform_file, "r"), Loader=Loader)        
        self._transforms = []
        sys.stderr.write(f"Transforms that will be applied:\n")
        for t_name in t_list:
            f = t_lib.get(t_name)
            if f is not None:
                sys.stdout.write(f"{t_name}: {f.__doc__}\n")
                self._transforms += [f]
            else:
                sys.stdout.write(f"{t_name}: Unknown transform!\n")

    def __call__(self, case: dict) -> dict:
        t_case = {}
        for t in self._transforms:
            t_case = t(t_case, case)
            # Parameter passing?
            # Transform error logging?
        return t_case

    @staticmethod
    def available_transforms():
        sys.stdout.write("\n".join([f"{k}: {v.__doc__}" for k, v in t_lib.items()]))
