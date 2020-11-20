import argparse
import json
import jsonlines
import sys
import time

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
#    parser.add_argument("--skipfile", help="File describing transforms to skip.")
    parser.add_argument("--limit", help="If present, will transform only the first N entries.")

    args = parser.parse_args()

    in_file, out_file, tranforms_file, limit = \
        args.input, args.output, args.transforms, args.limit

    transform = Transform(tranforms_file)

    t0 = time.time()
    with jsonlines.open(in_file) as dc_data_dump:
        with jsonlines.open(out_file, "w") as transformed_out:
            for n, case in enumerate(dc_data_dump):
                if limit is not None:
                    if n >= int(limit):
                        break

                if (n + 1) % 5000 == 0:
                    sys.stderr.write(f"Processed {n + 1} cases ({time.time() - t0}).\n")

                transformed_out.write(transform(case))
