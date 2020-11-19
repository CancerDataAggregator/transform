from typing import Union

import yaml
from yaml import Loader

from .transformlib import transform_library


class Transform:
    def __init__(self, transform_file) -> None:
        self._transforms = yaml.load(open(transform_file, "r"), Loader=Loader)

    def __call__(self, case: dict) -> dict:
        return self.r_transform(case)

    def r_transform(self, case, transform: dict = None) -> dict:
        if transform is None:
            # Top level
            transform = self._transforms
        
        if isinstance(case, list):
            return [self.r_transform(_c, transform) for _c in case]
        
        if isinstance(case, dict):
            ret = {}
            for k, v in case.items():
                if k not in transform:
                    # Skip this field
                    continue

                transforms_to_do = transform[k]["CDA-X"]

                field_transforms = transforms_to_do.get("field")
                if field_transforms is not None:
                    new_k = self.apply_transform(k, field_transforms)
                else:
                    new_k = k

                value_transforms = transforms_to_do.get("value")
                if value_transforms is not None:
                    # This automatically means this is a value and not a struct
                    new_value = self.apply_transform(v, value_transforms)
                else:
                    child_transforms = transform[k].get("CHILD")
                    if child_transforms is None:
                        # This is a simple value
                        new_value = v
                    else:
                        new_value = self.r_transform(v, child_transforms)

                ret[new_k] = new_value
            return ret

        raise RuntimeError(f"There is an error in the transforms dictionary: {str(transform)}")

    def apply_transform(self, k, tr_list):
        new_k = k
        for _t in tr_list:
            new_k = transform_library[_t["F"]](new_k, **_t.get("P", {}))
        return new_k

