import argparse
import logging

logger = logging.getLogger(__name__)


def main():

    logging.basicConfig()
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Input data file.")
    parser.add_argument("transforms", help="Transform definition file")
    parser.add_argument("--skipfile", help="File describing transforms to skip.")

    args = parser.parse_args()

    in_file, tranforms_file, skip_file = args.input, args.transforms, args.skip_file
