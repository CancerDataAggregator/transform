# Simple script to check GDC pull and verify contents

import gzip
import jsonlines
import argparse

from cdatransform.lib import get_ids


def check_case_375436b3(d):
    assert d.get("case_id") == "375436b3-66ac-4d5e-b495-18a96d812a69"
    assert d.get("submitter_id") == "TCGA-F5-6810"
    assert len(d.get("samples")) == 3
    assert d.get("disease_type") == "Adenomas and Adenocarcinomas"
    assert d.get("project").get("project_id") == "TCGA-READ"


def check_file_055a9c00(d):
    assert d.get("file_id") == "055a9c00-0a72-4005-83e2-457f56db4ad0"
    assert d.get("data_format") == "MAF"
    assert (
        d.get("file_name")
        == "b4cae59e-7128-49ed-829f-c121b4dd51de.wxs.VarScan2.aliquot.maf.gz"
    )
    assert d.get("submitter_id") == "8110e655-0d95-42b7-bb86-6cfce3e4d09c"
    assert d.get("md5sum") == "67fa80ad9e5a65b46df4fbe96f437b29"


def main():
    parser = argparse.ArgumentParser(description="Check GDC pull.")
    parser.add_argument("pulled_file", help="File produced by extract-gdc.")
    parser.add_argument("id_file", help="list of ids pulled")
    args = parser.parse_args()

    ids = set(get_ids(id=None, id_list_file=args.id_file))

    with gzip.open(args.pulled_file, "rb") as f_in:
        reader = jsonlines.Reader(f_in)
        for d in reader:
            assert d.get("id") in ids
            if d.get("case_id") == "375436b3-66ac-4d5e-b495-18a96d812a69":
                check_case_375436b3(d)
            if d.get("file_id") == "055a9c00-0a72-4005-83e2-457f56db4ad0":
                check_file_055a9c00(d)


main()
