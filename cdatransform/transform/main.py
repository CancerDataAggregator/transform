import argparse
import json
import jsonlines
import sys
import time
import gzip
import logging

from .extract.lib import get_case_ids
from .transform import Transform

logger = logging.getLogger(__name__)


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
