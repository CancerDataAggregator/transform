# Simple script to check GDC pull and verify contents

import gzip
import jsonlines
import argparse

from cdatransform.lib import get_case_ids


def check_case_00dc49de(d):
    assert d.get("case_id") == "00dc49de-1fba-11e9-b7f8-0a80fada099c"
    assert d.get("case_submitter_id") == "NCI7-2"
    assert len(d.get("demographics")) == 1
    assert len(d.get("samples")) == 1
    assert d.get("samples")[0].get("File")[0].get("file_id") is not None


def main():
    parser = argparse.ArgumentParser(description="Check GDC pull.")
    parser.add_argument("pull_file", help="File produced by extract-gdc.")
    parser.add_argument("case_file", help="File produced by .")
    args = parser.parse_args()

    case_ids = set(get_case_ids(case=None, case_list_file=args.case_file))

    with gzip.open(args.pull_file, "rb") as f_in:
        reader = jsonlines.Reader(f_in)
        for d in reader:
            assert d.get("case_id") in case_ids
            if d.get("case_id") == "00dc49de-1fba-11e9-b7f8-0a80fada099c":
                check_case_00dc49de(d)


main()
