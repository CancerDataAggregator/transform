import argparse
import json
import jsonlines
import sys

import logging

from .transform import Transform

logger = logging.getLogger(__name__)


def main():

    logging.basicConfig()
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Input data file.")
    parser.add_argument("output", help="Output data file.")
    parser.add_argument("transforms", help="Transform definition file")
    parser.add_argument("--skipfile", help="File describing transforms to skip.")

    args = parser.parse_args()

    in_file, out_file, tranforms_file, skip_file = \
        args.input, args.output, args.transforms, args.skipfile

    transform = Transform(tranforms_file)

    with jsonlines.open(in_file) as dc_data_dump:
        with jsonlines.open(out_file, "w") as transformed_out:
            for case in dc_data_dump:
                transformed_out.write(transform(case))
                break
