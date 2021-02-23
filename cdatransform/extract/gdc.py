import json
import jsonlines
import gzip
import sys
import time
import argparse
from collections import defaultdict
import pathlib

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
    "samples.portions.portion_id",
    "samples.portions.slide_id",
    "samples.portions.analytes.analyte_id",
    "samples.portions.analytes.aliquots.aliquot_id",
    "files.file_id",
    "files.data_category",
    "files.data_type",
    "files.file_name",
    "files.file_size",
    "files.md5sum",
    "files.data_format",
]

case_fields_to_use = [
    "case_id",
    "submitter_id",
    "disease_type",
    "primary_site",
    "project",
    "demographic",
    "diagnoses",
    "samples",
]

files_fields = ["file_id", "cases.samples.sample_id"]
gdc_files_page_size = 10000


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
        cache_file,
        gcs_file,
        cases_endpoint="https://api.gdc.cancer.gov/v0/cases",
        files_endpoint="https://api.gdc.cancer.gov/v0/files",
    ) -> None:
        self.cases_endpoint = cases_endpoint
        self.files_endpoint = files_endpoint
        self._samples_per_files_dict = self._fetch_file_data_from_cache(cache_file)
        self._fileuuid_to_gcs_mapping = self._fetch_file_id_to_gcs_mapping(gcs_file)

    def save_cases(self, out_file, case_ids=None, page_size=1000):
        t0 = time.time()
        n = 0
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            for case in self._cases(case_ids, page_size):
                case_with_specimen_files = self._attach_file_metadata(case)
                writer.write(case_with_specimen_files)
                n += 1
                if n % page_size == 0:
                    sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")

    def _cases(
        self,
        case_ids=None,
        page_size=100,
    ):
        fields = ",".join(cases_fields)
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
        while True:
            params = {
                "filters": filt,
                "format": "json",
                "fields": fields,
                "size": page_size,
                "from": offset,
            }

            # How to handle errors
            result = retry_get(self.cases_endpoint, params=params)
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

    def _attach_file_metadata(self, case_record) -> dict:
        new_case_record = {
            new_field: case_record.get(new_field)
            for new_field in case_fields_to_use
            if case_record.get(new_field) is not None
        }

        case_files_dict = {
            f.get("file_id"): f
            for f in case_record.get("files", [])  # [{"file_id": ..., ...}]
        }

        # By following condition, adding files for
        # cases that don't have samples is skipped
        for sample in new_case_record.get("samples", []):
            sample_id = sample.get("sample_id")
            file_ids = self._samples_per_files_dict.get(sample_id, [])

            sample["files"] = [
                self._attach_download_link(f_obj)
                for f_obj in (case_files_dict.get(f_id) for f_id in file_ids)
                if f_obj is not None
            ]

        return new_case_record

    def _attach_download_link(self, f_obj):
        f_obj.update(
            {"gcs_path": self._fileuuid_to_gcs_mapping.get(f_obj.get("file_id"))}
        )
        return f_obj

    def _fetch_file_data_from_cache(self, cache_file) -> dict:
        # The return is a dictionary sample_id: [file_ids]
        if not cache_file.exists():
            sys.stderr.write(f"Cache file {cache_file} not found. Generating.\n")
            gdc_files_metadata = []
            with gzip.open(cache_file, "w") as f_out:
                writer = jsonlines.Writer(f_out)
                for _meta in self._get_gdc_files():
                    writer.write(_meta)
                    gdc_files_metadata += [_meta]
        else:
            sys.stderr.write(f"Loading files metadata from cache file {cache_file}.\n")
            with gzip.open(cache_file, "rb") as f_in:
                reader = jsonlines.Reader(f_in)
                gdc_files_metadata = [f for f in reader]

        # Convert GDC return format to files_per_sample_dict
        files_per_sample_dict = defaultdict(list)
        for file_meta in gdc_files_metadata:
            for case in file_meta.get("cases", []):
                for sample in case.get("samples", []):
                    files_per_sample_dict[sample.get("sample_id")] += [
                        file_meta.get("file_id")
                    ]

        return files_per_sample_dict

    def _get_gdc_files(self):
        fields = ",".join(files_fields)
        total_files = get_total_number(self.files_endpoint)

        for offset in range(0, total_files, gdc_files_page_size):
            params = {
                "format": "json",
                "fields": fields,
                "sort": "file_id",
                "from": offset,
                "size": gdc_files_page_size,
            }
            sys.stderr.write(
                f"Pulling files page {int(offset / gdc_files_page_size) + 1}/{int(total_files / gdc_files_page_size) + 1}\n"
            )
            result = retry_get(self.files_endpoint, params=params)
            for hit in result.json()["data"]["hits"]:
                yield hit

    def _fetch_file_id_to_gcs_mapping(self, gcs_file) -> dict:
        # The return is a dictionary sample_id: [file_ids]
        if not gcs_file.exists():
            msg = f"File_id to GCS cache file {gcs_file} not found.\n"
            sys.stderr.write(msg)
            raise RuntimeError(msg)

        sys.stderr.write(f"Loading download links from cache file {gcs_file}.\n")
        with gzip.open(gcs_file, "rb") as f_in:
            reader = jsonlines.Reader(f_in)
            files_mapping = {f["file_uuid"]: f["gcs_path"] for f in reader}

        return files_mapping


def main():
    parser = argparse.ArgumentParser(description="Pull case data from GDC API.")
    parser.add_argument("out_file", help="Out file name. Should end with .gz")
    parser.add_argument("cache_file", help="Use (or generate if missing) cache file.")
    parser.add_argument("gcs_file", help="Use (or generate if missing) cache file.")
    parser.add_argument("--case", help="Extract just this case")
    parser.add_argument(
        "--cases", help="Optional file with list of case ids (one to a line)"
    )
    parser.add_argument("--cache", help="Use cached files.", action="store_true")
    args = parser.parse_args()

    gdc = GDC(
        cache_file=pathlib.Path(args.cache_file), gcs_file=pathlib.Path(args.gcs_file)
    )
    gdc.save_cases(
        args.out_file, case_ids=get_case_ids(case=args.case, case_list_file=args.cases)
    )


if __name__ == "__main__":
    main()
