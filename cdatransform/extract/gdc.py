import requests
import json
import jsonlines
import gzip
import sys
import time
import argparse

from cdatransform.lib import get_case_ids
from cdatransform.extract.lib import retry_get


default_fields = [
    "case_id",
    "submitter_id",
    "disease_type",
    "primary_site",
    "project.project_id",
    "demographic.ethnicity",
    "demographic.gender",
    "demographic.race",
    "demographic.days_to_birth",
    "diagnoses.diagnosis_id",
    "diagnoses.age_at_diagnosis",
    "diagnoses.tumor_grade",
    "diagnoses.tumor_stage",
    "diagnoses.morphology",
    "diagnoses.primary_diagnosis",
    "diagnoses.treatments.treatment_outcome",
    "diagnoses.treatments.treatment_type",
    "samples.sample_id",
    "samples.submitter_id",
    "samples.sample_type",
    "samples.biospecimen_anatomic_site",
    "samples.portions.portion_id",
    "samples.portions.slide_id",
    "samples.portions.analytes.analyte_id",
    "samples.portions.analytes.aliquots.aliquot_id",
    "files.file_id",
    "files.file_name",
    "files.file_size",
    "files.platform",
    "files.revision",
    "files.tags",
    "files.type",
]


def clean_fields(hit):
    if hit.get("age_at_diagnosis") is not None:
        hit["age_at_diagnosis"] = int(hit.get("age_at_diagnosis"))
    return hit


class GDC:
    def __init__(
        self, endpoint="https://api.gdc.cancer.gov/v0/cases", fields=default_fields
    ) -> None:
        self.endpoint = endpoint
        self.fields = fields

    def cases(
        self,
        case_ids=None,
        page_size=100,
    ):
        fields = ",".join(self.fields)
        # defining the GDC API query
        if case_ids is not None:
            filt = json.dumps(
                {
                    "op": "and",
                    "content": [
                        {"op": "in", "content": {"field": "case_id", "value": case_ids}}
                    ],
                }
            )
        else:
            filt = None

        offset = 0
        t0 = time.time()
        while True:
            params = {
                "filters": filt,
                "format": "json",
                "fields": fields,
                "size": page_size,
                "from": offset,
            }

            # How to handle errors
            result = retry_get(self.endpoint, params=params)
            hits = result.json()["data"]["hits"]
            page = result.json()["data"]["pagination"]
            p_no = page.get("page")
            p_tot = page.get("pages")

            sys.stderr.write(f"Pulling page {p_no} / {p_tot}\n")

            for hit in hits:
                yield clean_fields(hit)

            if p_no >= p_tot:
                break
            else:
                offset += page_size

    def save_cases(self, out_file, case_ids=None, page_size=1000):
        t0 = time.time()
        n = 0
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            for case in self.cases(case_ids, page_size):
                writer.write(case)
                n += 1
                if n % page_size == 0:
                    sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")


def main():
    parser = argparse.ArgumentParser(description="Pull case data from GDC API.")
    parser.add_argument("out_file", help="Out file name. Should end with .gz")
    parser.add_argument("--case", help="Extract just this case")
    parser.add_argument(
        "--cases", help="Optional file with list of case ids (one to a line)"
    )
    args = parser.parse_args()

    gdc = GDC()
    gdc.save_cases(
        args.out_file, case_ids=get_case_ids(case=args.case, case_list_file=args.cases)
    )


if __name__ == "__main__":
    main()
