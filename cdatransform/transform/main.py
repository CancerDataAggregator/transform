import argparse
import json
import jsonlines
import sys
import time
import gzip
import logging
from typing import Union

import yaml
from yaml import Loader

from cdatransform.extract.lib import get_case_ids
from .gdclib import (research_subject, diagnosis, entity_to_specimen)

logger = logging.getLogger(__name__)

t_lib = {
    "gdc.research_subject": research_subject,
    "gdc.diagnosis": diagnosis,
    "gdc.entity_to_specimen": entity_to_specimen
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


def main():

    parser = argparse.ArgumentParser(prog="Transform", description="Transform source DC jsonl to Harmonized jsonl")
    parser.add_argument("input", help="Input data file.")
    parser.add_argument("output", help="Output data file.")
    parser.add_argument("transforms", help="Transforms list file.")
    parser.add_argument("--log", default="transform.log", help="Name of log file.")
    parser.add_argument('--case', help='Transform just this case')
    parser.add_argument('--cases', help='Optional file with list of case ids (one to a line)')

    args = parser.parse_args()

    in_file, out_file, t_file, log_file, case, case_f = \
        args.input, args.output, args.transforms, args.log, args.case, args.cases

    logging.basicConfig(
        filename=log_file, 
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO)
    logger.info("----------------------")
    logger.info("Starting transform run")
    logger.info("----------------------")

    transform = Transform(t_file)

    t0 = time.time()
    count = 0
    case_list = None
    if case is not None:
        case_list = set(case)
    elif case_f is not None:
        case_list = get_case_ids(case_list_file=case_f)

    with gzip.open(in_file, "r") as infp:
        with gzip.open(out_file, "w") as outfp:
            reader = jsonlines.Reader(infp)
            writer = jsonlines.Writer(outfp)
            for case in reader:
                if case_list is not None:
                    if len(case_list) == 0:
                        break
                    if case.get("id") not in case_list:
                        continue
                    case_list.pop(case.get("id"))

                writer.write(transform(case))
                count += 1                
                if count % 5000 == 0:
                    sys.stderr.write(f"Processed {count} cases ({time.time() - t0}).\n")

    sys.stderr.write(f"Processed {count} cases ({time.time() - t0}).\n")
