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
    "project.dbgap_accession_number",
    "demographic.ethnicity",
    "demographic.gender",
    "demographic.race",
    "demographic.days_to_birth",
    "demographic.days_to_death",
    "demographic.cause_of_death",
    "demographic.vital_status",
    "diagnoses.diagnosis_id",
    "diagnoses.age_at_diagnosis",
    "diagnoses.tumor_grade",
    "diagnoses.tumor_stage",
    "diagnoses.morphology",
    "diagnoses.primary_diagnosis",
    "diagnoses.method_of_diagnosis"
    "diagnoses.treatments.treatment_outcome",
    "diagnoses.treatments.treatment_type",
    "diagnoses.treatments.treatment_id",
    "diagnoses.treatments.days_to_treatment",
    "diagnoses.treatments.days_to_treatment_end",
    "diagnoses.treatments.therapeutic_agents",
    "diagnoses.treatments.treatment_anatomic_site",
    "diagnoses.treatments.treatment_effect",
    "diagnoses.treatments.reason_treatment_ended",
    "diagnoses.treatments.number_of_cycles",
    "samples.sample_id",
    "samples.submitter_id",
    "samples.sample_type",
    "samples.current_weight",
    "samples.initial_weight",
    "samples.days_to_collection",
    "samples.days_to_sample_procurement",
    "samples.passage_count",
    "samples.biospecimen_anatomic_site",
    "samples.portions.portion_id",
    "samples.portions.submitter_id",
    "samples.portions.slides.slide_id",
    "samples.portions.slides.submitter_id",
    "samples.portions.analytes.analyte_id",
    "samples.portions.analytes.submitter_id",
    "samples.portions.analytes.aliquots.aliquot_id",
    "samples.portions.analytes.aliquots.submitter_id",
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
    "files",
]

files_fields = [
    "file_id",
    "data_category",
    "data_type",
    "file_name",
    "file_size",
    "md5sum",
    "data_format",
    "cases.case_id",
    "cases.submitter_id",
    "cases.project.project_id",
    "cases.project.dbgap_accession_number",
    "cases.disease_type",
    "cases.primary_site",
    "cases.demographic.ethnicity",
    "cases.demographic.gender",
    "cases.demographic.race",
    "cases.demographic.days_to_birth",
    "cases.demographic.days_to_death",
    "cases.demographic.cause_of_death",
    "cases.demographic.vital_status",
    "cases.diagnoses.diagnosis_id",
    "cases.diagnoses.age_at_diagnosis",
    "cases.diagnoses.tumor_grade",
    "cases.diagnoses.tumor_stage",
    "cases.diagnoses.morphology",
    "cases.diagnoses.primary_diagnosis",
    "cases.diagnoses.treatments.treatment_outcome",
    "cases.diagnoses.treatments.treatment_type",
    "cases.diagnoses.treatments.treatment_id",
    "cases.diagnoses.treatments.days_to_treatment",
    "cases.diagnoses.treatments.days_to_treatment_end",
    "cases.diagnoses.treatments.therapeutic_agents",
    "cases.diagnoses.treatments.treatment_anatomic_site",
    "cases.diagnoses.treatments.treatment_effect",
    "cases.diagnoses.treatments.reason_treatment_ended",
    "cases.diagnoses.treatments.number_of_cycles",
    "cases.samples.sample_id",
    "cases.samples.submitter_id",
    "cases.samples.sample_type",
    "cases.samples.current_weight",
    "cases.samples.initial_weight",
    "cases.samples.days_to_collection",
    "cases.samples.days_to_sample_procurement",
    "cases.samples.passage_count",
    "cases.samples.biospecimen_anatomic_site",
    "cases.samples.portions.portion_id",
    "cases.samples.portions.submitter_id",
    "cases.samples.portions.slides.slide_id",
    "cases.samples.portions.slides.submitter_id",
    "cases.samples.portions.analytes.analyte_id",
    "cases.samples.portions.analytes.submitter_id",
    "cases.samples.portions.analytes.aliquots.aliquot_id",
    "cases.samples.portions.analytes.aliquots.submitter_id",
]
# What is the significance of cases.samples.sample_id vs cases.sample_ids?
# Answer: cases.sample_ids is not returned by GDC API 
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
        cases_endpoint="https://api.gdc.cancer.gov/v0/cases",
        files_endpoint="https://api.gdc.cancer.gov/v0/files",
    ) -> None:
        self.cases_endpoint = cases_endpoint
        self.files_endpoint = files_endpoint
        self._samples_per_files_dict = self._fetch_file_data_from_cache(cache_file)

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

    def save_files(self, out_file, cache_file, file_ids=None):
        t0 = time.time()
        n = 0
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            print(cache_file)
            with gzip.open(cache_file, "r") as fr:
                reader = jsonlines.Reader(fr)
                if file_ids:
                    print(file_ids)
                    for file in reader:
                        if file.get('file_id') in file_ids:
                            writer.write(file)
                            n += 1
                else:
                    for file in reader:
                        writer.write(file)
                        n += 1
        sys.stderr.write(f"Extracted {n} files from cache in {time.time() - t0}s\n")

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
                f_obj
                for f_obj in (case_files_dict.get(f_id) for f_id in file_ids)
                if f_obj is not None
            ]
            for portion in sample.get("portions", []):
                portion_id = portion.get("portion_id")
                file_ids = self._samples_per_files_dict.get(portion_id, [])

                portion["files"] = [
                    f_obj
                    for f_obj in (case_files_dict.get(f_id) for f_id in file_ids)
                    if f_obj is not None
                ]
                for slide in portion.get("slides", []):
                    slide_id = slide.get("slide_id")
                    file_ids = self._samples_per_files_dict.get(slide_id, [])

                    slide["files"] = [
                        f_obj
                        for f_obj in (case_files_dict.get(f_id) for f_id in file_ids)
                        if f_obj is not None
                    ]
                for analyte in portion.get("analytes", []):
                    analyte_id = analyte.get("analyte_id")
                    file_ids = self._samples_per_files_dict.get(analyte_id, [])

                    analyte["files"] = [
                        f_obj
                        for f_obj in (case_files_dict.get(f_id) for f_id in file_ids)
                        if f_obj is not None
                    ]
                    for aliquot in analyte.get("aliquots", []):
                        aliquot_id = aliquot.get("aliquot_id")
                        file_ids = self._samples_per_files_dict.get(aliquot_id, [])
                        aliquot["files"] = [
                            f_obj
                            for f_obj in (
                                case_files_dict.get(f_id) for f_id in file_ids
                            )
                            if f_obj is not None
                        ]

        return new_case_record

    def _fetch_file_data_from_cache(self, cache_file) -> dict:
        # The return is a dictionary sample_id: [file_ids]
        if not cache_file.exists():
            sys.stderr.write(f"Cache file {cache_file} not found. Generating.\n")
            gdc_files_metadata = []
            with gzip.open(cache_file, "wt") as f_out:
                writer = jsonlines.Writer(f_out)
                for _meta in self._get_gdc_files():
                    writer.write(_meta)
                    gdc_files_metadata += [_meta]
        else:
            sys.stderr.write(f"Loading files metadata from cache file {cache_file}.\n")
            with gzip.open(cache_file, "rt") as f_in:
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
                    for portion in sample.get("portions", []):
                        files_per_sample_dict[portion.get("portion_id")] += [
                            file_meta.get("file_id")
                        ]
                        for slide in portion.get("slides", []):
                            files_per_sample_dict[slide.get("slide_id")] += [
                                file_meta.get("file_id")
                            ]
                        for analyte in portion.get("analytes", []):
                            files_per_sample_dict[analyte.get("analyte_id")] += [
                                file_meta.get("file_id")
                            ]
                            for aliquot in analyte.get("aliquots", []):
                                files_per_sample_dict[aliquot.get("aliquot_id")] += [
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


def main():
    parser = argparse.ArgumentParser(description="Pull case data from GDC API.")
    parser.add_argument("out_file", help="Out file name. Should end with .gz")
    parser.add_argument("cache_file", help="Use (or generate if missing) cache file.")
    parser.add_argument("--case", help="Extract just this case")
    parser.add_argument(
        "--cases", help="Optional file with list of case ids (one to a line)"
    )
    parser.add_argument("--file", help="Extract just this file")
    parser.add_argument(
        "--files", help="Optional file with list of file ids (one to a line)"
    )
    parser.add_argument("--cache", help="Use cached files.", action="store_true")
    parser.add_argument(
        "--endpoint", help="Extract all from 'files' or 'cases' endpoint "
    )
    args = parser.parse_args()

    gdc = GDC(cache_file=pathlib.Path(args.cache_file))
    if args.case or args.cases or args.endpoint=='cases':
        gdc.save_cases(
            args.out_file, case_ids=get_case_ids(case=args.case, case_list_file=args.cases)
        )
    if args.file or args.files or args.endpoint=='files':
        gdc.save_files(
            args.out_file, args.cache_file, file_ids=get_case_ids(case=args.file, case_list_file=args.files)
        )

if __name__ == "__main__":
    main()
