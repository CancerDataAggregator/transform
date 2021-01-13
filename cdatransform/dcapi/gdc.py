import requests
import json
import jsonlines
import gzip
import sys
import time

from .lib import retry_get


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
        if case_ids is not None:
            # defining the GDC API query
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
        n = 0
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

            sys.stderr.write(f"Pulling page {p_no} / {p_tot}. {n} cases done in {time.time() - t0}s\n")

            for hit in hits:
                n += 1
                yield hit

            if p_no >= p_tot:
                break
            else:
                offset += page_size


    def save_cases(self, out_file, case_ids=None, page_size=1000):
        with gzip.open(out_file, 'wb') as fp:
            writer = jsonlines.Writer(fp)
            for case in self.cases(case_ids, page_size):
                writer.write(case)


def main():
    gdc = GDC()
    gdc.save_cases("gdc.jsonl.gz")


if __name__ == "__main__":
    main()
