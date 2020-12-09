# Common or utility transform functions go here

from collections import namedtuple
from typing import Union


XFunc = namedtuple('XFunc', ['func', 'params'])


def apply_on_column(input: dict, path: str, func: XFunc) -> dict:
    _recurse_apply(input, path.split("."), func)
    return input


def _recurse_apply(fragment: Union[dict, list], path: list, func: XFunc):
    if isinstance(fragment, list):
        for _f in fragment:
            _recurse_apply(_f, path, func)
    else:
        if path[0] in fragment:
            new_fragment = fragment[path[0]]
            if len(path) == 1:
                func.func(new_fragment, func.params)
            else:
                _recurse_apply(new_fragment, path[1:], func)
