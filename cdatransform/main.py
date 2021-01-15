import argparse
import json
import jsonlines
import sys
import time
import gzip
import logging

from .transform import Transform

logger = logging.getLogger(__name__)


def main():

    parser = argparse.ArgumentParser(prog="Transform", description="Transform source DC jsonl to Harmonized jsonl")
    parser.add_argument("input", help="Input data file.")
    parser.add_argument("output", help="Output data file.")
    parser.add_argument("transforms", help="Transforms list file.")
    parser.add_argument("--log", default="transform.log", help="Name of log file.")
    parser.add_argument("--limit", help="If present, will transform only the first N entries.")

    args = parser.parse_args()

    in_file, out_file, t_file, log_file, limit = \
        args.input, args.output, args.transforms, args.log, int(args.limit or 0)

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
    with gzip.open(in_file, "r") as infp:
        with gzip.open(out_file, "w") as outfp:
            reader = jsonlines.Reader(infp)
            writer = jsonlines.Writer(outfp)
            for case in reader:
                writer.write(transform(case))
                count += 1                
                if count == limit:
                    break
                if count % 5000 == 0:
                    sys.stderr.write(f"Processed {count} cases ({time.time() - t0}).\n")

    sys.stderr.write(f"Processed {count} cases ({time.time() - t0}).\n")
