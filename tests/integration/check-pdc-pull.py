# Simple script to check GDC pull and verify contents

import gzip
import jsonlines
import argparse

from cdatransform.lib import get_ids


def check_case_00dc49de(d):
    # print(d)
    assert d.get("case_id") == "00dc49de-1fba-11e9-b7f8-0a80fada099c"
    assert d.get("case_submitter_id") == "NCI7-2"
    assert len(d.get("demographics")) == 1
    assert len(d.get("samples")) == 1
    assert d.get("project_submitter_id") == "CPTAC3-Discovery"


def check_file_049dbc20(d):
    assert d.get("file_id") == "049dbc20-19cd-11e9-99db-005056921935"
    assert d.get("file_format") == "vendor-specific"
    assert d.get("file_name") == "08CPTAC_CCRCC_W_JHU_20171205_LUMOS_f19.raw"
    assert d.get("data_category") == "Raw Mass Spectra"
    assert d.get("md5sum") == "121ca7617f81fe47f1cf8375d4622fbb"


def main():
    parser = argparse.ArgumentParser(description="Check GDC pull.")
    parser.add_argument("pulled_file", help="File produced by extract-gdc.")
    parser.add_argument("id_file", help="File of ids extracted .")
    args = parser.parse_args()

    ids = set(get_ids(id=None, id_list_file=args.id_file))

    with gzip.open(args.pulled_file, "rb") as f_in:
        reader = jsonlines.Reader(f_in)
        for d in reader:
            try:
                assert d.get("case_id") in ids
            except:
                assert d.get("file_id") in ids
            if d.get("case_id") == "00dc49de-1fba-11e9-b7f8-0a80fada099c":
                check_case_00dc49de(d)
            if d.get("file_id") == "049dbc20-19cd-11e9-99db-005056921935":
                check_file_049dbc20(d)


main()
