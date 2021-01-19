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

from cdatransform.lib import get_case_ids
from cdatransform.transform.lib import Transform


logger = logging.getLogger(__name__)


def filter_cases(reader, case_list):
    if case_list is None:
        cases = None
    else:
        cases = set(case_list)

    for case in reader:
        if cases is None:
            yield case
        elif len(cases) == 0:
            break
        else:
            if case.get("id") in cases:
                cases.pop(case.get("id"))
                yield case


def main():

    parser = argparse.ArgumentParser(
        prog="Transform", description="Transform source DC jsonl to Harmonized jsonl"
    )
    parser.add_argument("input", help="Input data file.")
    parser.add_argument("output", help="Output data file.")
    parser.add_argument("transforms", help="Transforms list file.")
    parser.add_argument("--log", default="transform.log", help="Name of log file.")
    parser.add_argument("--case", help="Transform just this case")
    parser.add_argument(
        "--cases", help="Optional file with list of case ids (one to a line)"
    )
    args = parser.parse_args()


    logging.basicConfig(
        filename=args.log,
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
    )
    logger.info("----------------------")
    logger.info("Starting transform run")
    logger.info("----------------------")

    transform = Transform(args.transforms)

    t0 = time.time()
    count = 0
    case_list = get_case_ids(case=args.case, case_list_file=args.cases)

    with gzip.open(args.input, "r") as infp:
        with gzip.open(args.output, "w") as outfp:
            reader = jsonlines.Reader(infp)
            writer = jsonlines.Writer(outfp)
            for case in filter_cases(reader, case_list=case_list):
                writer.write(transform(case))
                count += 1
                if count % 5000 == 0:
                    sys.stderr.write(f"Processed {count} cases ({time.time() - t0}).\n")

    sys.stderr.write(f"Processed {count} cases ({time.time() - t0}).\n")
