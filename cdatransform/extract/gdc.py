import json
import jsonlines
import gzip
import sys
import time
import argparse
from collections import defaultdict
import os

from cdatransform.lib import get_case_ids
from cdatransform.extract.lib import retry_get


cases_fields = [
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
    "files.file_id",
    "files.file_name",
    "files.file_size",
    "files.platform",
    "files.revision",
    "files.tags",
    "files.type",
    "files.data_type",
]

files_fields = ["file_id", "cases.samples.sample_id"]


def clean_fields(hit):
    if hit.get("age_at_diagnosis") is not None:
        hit["age_at_diagnosis"] = int(hit.get("age_at_diagnosis"))
    return hit


def get_total_number(endpoint):
    params = {"format": "json"}
    result = retry_get(endpoint, params=params)
    return result.json()["data"]["pagination"]["total"]


class GDC:
    def __init__(
        self,
        cases_endpoint="https://api.gdc.cancer.gov/v0/cases",
        files_endpoint="https://api.gdc.cancer.gov/v0/files",
        cases_fields=cases_fields,
        files_fields=files_fields,
        cached_files="pdc_files_cached.json.gz",
    ) -> None:

        self.cases_endpoint = cases_endpoint
        self.files_endpoint = files_endpoint
        self.cases_fields = cases_fields
        self.files_fields = files_fields
        self.cached_files = cached_files

    def cases(
        self,
        case_ids=None,
        page_size=100,
    ):
        fields = ",".join(self.cases_fields)
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

        total_cases = get_total_number(self.cases_endpoint)
        t0 = time.time()

        for offset in range(0, total_cases, page_size):
            params = {
                "filters": filt,
                "format": "json",
                "fields": fields,
                "size": page_size,
                "from": offset,
            }

            sys.stderr.write(
                f"Pulling cases page {int(offset / page_size) + 1}/{int(total_cases / page_size) + 1}\n"
            )
            result = retry_get(self.cases_endpoint, params=params)
            hits = result.json()["data"]["hits"]

            for hit in hits:
                yield clean_fields(hit)

    def files(self, cached=False, page_size=10000):
        if cached:
            if not os.path.isfile(self.cached_files):
                sys.stderr.write("Cached file not found. Pulling from the API.\n")
                pull_from_api = True
            else:
                pull_from_api = False
        else:
            pull_from_api = True

        if pull_from_api:
            fields = ",".join(self.files_fields)
            total_files = get_total_number(self.files_endpoint)

            with gzip.open(self.cached_files, "wb") as cache_file:
                writer = jsonlines.Writer(cache_file)
                for offset in range(0, total_files, page_size):
                    params = {
                        "format": "json",
                        "fields": fields,
                        "sort": "file_id",
                        "from": offset,
                        "size": 10000,
                    }
                    sys.stderr.write(
                        f"Pulling files page {int(offset / page_size) + 1}/{int(total_files / page_size) + 1}\n"
                    )
                    result = retry_get(self.files_endpoint, params=params)
                    hits = result.json()["data"]["hits"]

                    for hit in hits:
                        writer.write(hit)
                        yield hit
        else:
            if os.path.isfile(self.cached_files):
                sys.stderr.write("Using cached files.\n")
                with gzip.open(self.cached_files, "rb") as cache_file:
                    for hit in cache_file:
                        yield json.loads(hit)

    def get_files_per_sample(self, cached=False) -> dict:
        files_per_sample = defaultdict(list)
        sys.stderr.write(f"Started collecting files.\n")

        t0 = time.time()

        for hit in self.files(cached=cached):
            file_id = hit.get("file_id")
            cases = hit.get("cases")
            if cases is not None:
                for case in cases:
                    samples = case["samples"]
                    if samples is not None:
                        for sample in case["samples"]:
                            sample_id = sample.get("sample_id")
                            files_per_sample[sample_id].append(file_id)

        sys.stderr.write(
            f"Created a files look-up dict for {len(files_per_sample)} samples in {time.time() - t0}s\n"
        )
        return files_per_sample

    def rearrange_fields(self, case_record, files_per_sample) -> dict:
        new_case_record = dict()
        new_case_fields = [
            field.split(".")[0]
            for field in self.cases_fields
            if not field.startswith("files.")
        ]
        for new_field in new_case_fields:
            if case_record.get(new_field) is not None:
                new_case_record[new_field] = case_record.get(new_field)

        samples = new_case_record.get("samples")
        # By following condition, adding files for
        # cases that don't have samples is skipped
        if samples:
            for index, sample in enumerate(new_case_record.get("samples")):
                new_case_record["samples"][index]["files"] = []
                sample_id = sample.get("sample_id")
                file_ids = files_per_sample.get(sample_id)
                case_files = case_record.get("files")
                if file_ids and case_files:
                    for f in case_files:
                        if f.get("file_id") in set(file_ids):
                            new_case_record["samples"][index]["files"].append(f)
        return new_case_record

    def save_cases(self, out_file, case_ids=None, cached=False, page_size=1000):
        t0 = time.time()
        n = 0
        files_per_sample = self.get_files_per_sample(cached)
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            for case in self.cases(case_ids, page_size):
                case_with_specimen_files = self.rearrange_fields(case, files_per_sample)
                writer.write(case_with_specimen_files)
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
    parser.add_argument("--cache", help="Use cached files.", action="store_true")
    args = parser.parse_args()

    gdc = GDC()
    gdc.save_cases(
        args.out_file,
        case_ids=get_case_ids(case=args.case, case_list_file=args.cases),
        cached=args.cache,
    )


if __name__ == "__main__":
    main()
