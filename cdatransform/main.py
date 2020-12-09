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
    parser.add_argument("transforms", help="Transforms list file.")
    parser.add_argument("--limit", help="If present, will transform only the first N entries.")

    args = parser.parse_args()

    in_file, out_file, t_file, limit = \
        args.input, args.output, args.transforms, int(args.limit or 0)

    transform = Transform(t_file)

    t0 = time.time()
    count = 0
    with jsonlines.open(in_file) as dc_data_dump:
        with jsonlines.open(out_file, "w") as transformed_out:
            for case in dc_data_dump:
                transformed_out.write(transform(case))
                count += 1                
                if count == limit:
                    break
                if count % 5000 == 0:
                    sys.stderr.write(f"Processed {count} cases ({time.time() - t0}).\n")

    sys.stderr.write(f"Processed {count} cases ({time.time() - t0}).\n")
