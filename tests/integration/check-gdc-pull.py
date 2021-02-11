# Simple script to check GDC pull and verify contents

import gzip
import jsonlines
import argparse

from cdatransform.lib import get_case_ids


def check_case_375436b3(d):
    assert d.get("case_id") == "375436b3-66ac-4d5e-b495-18a96d812a69"
    assert d.get("submitter_id") == "TCGA-F5-6810"
    assert len(d.get("samples")) == 3
    assert len(d.get("samples")[0].get("files")) == 1
    assert len(d.get("samples")[1].get("files")) == 21


def main():
    parser = argparse.ArgumentParser(description="Check GDC pull.")
    parser.add_argument("pull_file", help="File produced by extract-gdc.")
    parser.add_argument("case_file", help="File produced by .")
    args = parser.parse_args()

    case_ids=set(get_case_ids(case=None, case_list_file=args.case_file))

    with gzip.open(args.pull_file, "rb") as f_in:
        reader = jsonlines.Reader(f_in)
        for d in reader:
            assert d.get("case_id") in case_ids
            if d.get("case_id") == "375436b3-66ac-4d5e-b495-18a96d812a69":
                check_case_375436b3(d)


main()