import requests
import json
import jsonlines
import gzip
import sys
import time
import argparse

from .lib import get_case_ids, retry_get


default_fields = [
    "case_id",
    "consent_type",
    "disease_type",
    "primary_site",
    "submitter_id",
    "demographic.ethnicity",
    "demographic.gender",
    "demographic.race",
    "demographic.days_to_birth",
    "diagnoses.treatments.treatment_outcome",
    "diagnoses.treatments.treatment_type",
    "diagnoses.tumor_grade",
    "diagnoses.tumor_stage",
    "samples.sample_id",
    "samples.submitter_id",
    "samples.sample_type",
    "samples.tissue_type",
]


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
                yield hit

            if p_no >= p_tot:
                break
            else:
                offset += page_size


    def save_cases(self, out_file, case_ids=None, page_size=1000):
        t0 = time.time()
        n = 0
        with gzip.open(out_file, 'wb') as fp:
            writer = jsonlines.Writer(fp)
            for case in self.cases(case_ids, page_size):
                writer.write(case)
                n += 1
                if n % 100 == 0:
                    sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")


def main():
    parser = argparse.ArgumentParser(description='Pull case data from GDC API.')
    parser.add_argument('out_file', help='Out file name. Should end with .gz')
    parser.add_argument('--cases', help='Optional file with list of case ids (one to a line)')    
    args = parser.parse_args()

    gdc = GDC()
    gdc.save_cases(args.out_file, case_ids=get_case_ids(args.cases))


if __name__ == "__main__":
    main()
