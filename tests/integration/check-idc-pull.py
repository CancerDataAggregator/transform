# Simple script to check GDC pull and verify contents

import gzip
import jsonlines
import argparse

from cdatransform.lib import get_case_ids


def check_case_375436b3(d):
    assert d.get("crdc_instance_uuid") == "dg.4DFC/f178b884-99b4-4929-a5a5-bc226b27da50"
    assert d.get("PatientID") == "TCGA-G4-6304"
    assert d.get("source_DOI") == "10.7937/K9/TCIA.2016.HJJHBOXZ"
    assert d.get("Program") == "TCGA
    assert d.get("tcia_tumorLocation") == "Colon


def main():
    parser = argparse.ArgumentParser(description="Check IDC pull.")
    parser.add_argument("pull_file", help="File produced by extract-idc.")
    parser.add_argument("case_file", help="File produced by .")
    args = parser.parse_args()

    case_ids = set(get_case_ids(case=None, case_list_file=args.case_file))

    with gzip.open(args.pull_file, "rb") as f_in:
        reader = jsonlines.Reader(f_in)
        for d in reader:
            assert d.get("case_id") in case_ids
            if d.get("case_id") == "375436b3-66ac-4d5e-b495-18a96d812a69":
                check_case_375436b3(d)


main()
