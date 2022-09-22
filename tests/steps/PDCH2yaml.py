import sys
import jsonlines
import yaml
import gzip
import argparse


def main():
    parser = argparse.ArgumentParser(description="Check GDC pull.")
    parser.add_argument("in_file", help="File produced by extract-gdc.")
    parser.add_argument("out_file", help="File produced by .")
    args = parser.parse_args()

    with gzip.open(args.in_file, "rb") as f_in:
        reader = jsonlines.Reader(f_in)
        with open(args.out_file, "w") as outfile:
            for d in reader:
                yaml.dump([d], outfile)


main()
