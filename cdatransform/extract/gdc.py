import json
from typing import Iterable
import jsonlines
import gzip
import sys
import time
import argparse
from collections import defaultdict
import pathlib

from cdatransform.lib import get_case_ids
from cdatransform.extract.lib import retry_get


# What is the significance of cases.samples.sample_id vs cases.sample_ids?
# Answer: cases.sample_ids is not returned by GDC API
gdc_files_page_size = 8000


def clean_fields(hit) -> dict:
    if hit.get("age_at_diagnosis") is not None:
        hit["age_at_diagnosis"] = int(hit.get("age_at_diagnosis"))
    return hit


def get_total_number(endpoint) -> int:
    params = {"format": "json"}
    result = retry_get(endpoint, params=params)
    return result.json()["data"]["pagination"]["total"]


class GDC:
    def __init__(
        self,
        #cache_file: pathlib.Path,
        cases_endpoint: str = "https://api.gdc.cancer.gov/v0/cases",
        files_endpoint: str = "https://api.gdc.cancer.gov/v0/files",
        #parent_spec: bool = True,
        field_break: int = 100,
        fields: list = [],
    ) -> None:
        self.cases_endpoint = cases_endpoint
        self.files_endpoint = files_endpoint
        self.field_break = field_break
        self.fields = fields
        #self._samples_per_files_dict = self._fetch_file_data_from_cache(cache_file)
        #self.parent_spec = parent_spec

    def save_cases(
        self, out_file: str, case_ids: str = None, page_size: int = 1000
    ) -> None:
        t0 = time.time()
        n = 0
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            for case in self._cases(case_ids, page_size):
                writer.write(case)
                n += 1
                if n % page_size == 0:
                    sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")

    def save_files(
        self, out_file: str, file_ids: list = None, page_size: int = 5000
    ) -> None:
        t0 = time.time()
        n = 0
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            for file in self._files(file_ids, page_size):
                writer.write(file)
                n += 1
                if n % page_size == 0:
                    sys.stderr.write(f"Wrote {n} files in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} files in {time.time() - t0}s\n")
                #if file_ids:
                #    for file in reader:
                #        if file.get("file_id") in file_ids:
                #            writer.write(self.prune_specimen_tree(file))
                #            n += 1
                #else:
                #    for file in reader:
                #        # add parent specimens to list of associated entities. remove cases?
                #        if "associated_entities" not in file:
                #            print(file)
                #        writer.write(self.prune_specimen_tree(file))
                #        n += 1
        #sys.stderr.write(f"Extracted {n} files from cache in {time.time() - t0}s\n")

    def _cases(
        self,
        case_ids: list = None,
        page_size: int = 100,
    ) -> Iterable:
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
        offset: int = 0
        while True:
            fields = ','.join(self.fields[0:self.field_break])
            print(fields)
            params = {
                        "filters": filt,
                        "format": "json",
                        "fields": fields,
                        "size": page_size,
                        "from": offset,
                    }

            result = retry_get(self.cases_endpoint, params = params)
            hits = result.json()["data"]["hits"]
            result_dict = {hit["case_id"]:hit for hit in hits}
            params.update({'fields':','.join(["case_id"]+self.fields[self.field_break:])})
            result = retry_get(self.cases_endpoint, params = params)
            hits = result.json()["data"]["hits"]
            page = result.json()["data"]["pagination"]
            result_dict2 = {hit["case_id"]:hit for hit in hits}
            res_list = [result_dict[case].update(result_dict2[case]) for case in result_dict]
            for case in res_list:
                yield case
            p_no = page.get("page")
            p_tot = page.get("pages")
            sys.stderr.write(f"Pulling page {p_no} / {p_tot}\n")

            if p_no >= p_tot:
                break
            else:
                offset += page_size

    def _files(self,
        file_ids: list = None,
        page_size: int = 500,
    ) -> Iterable:
        if file_ids is not None:
            filt = json.dumps(
                {
                    "op": "and",
                    "content": [
                        {"op": "in", "content": {"field": "file_id", "value": file_ids}}
                    ],
                }
            )
        else:
            filt = None
        total_files = get_total_number(self.files_endpoint)
        offset: int = 0
        while True:
            fields = ','.join(self.fields)
            params = {
                        "filters": filt,
                        "format": "json",
                        "fields": fields,
                        "size": page_size,
                        "from": offset,
                        #"sort": "file_id",
                    }

            result = retry_get(self.files_endpoint, params = params)
            page = result.json()["data"]["pagination"]
            for hit in result.json()["data"]["hits"]:
                yield hit
            p_no = page.get("page")
            p_tot = page.get("pages")
            sys.stderr.write(f"Pulling page {p_no} / {p_tot}\n")

            if p_no >= p_tot:
                break
            else:
                offset += page_size
        #for offset in range(0, total_files, page_size):
        #    params = {
        #        "filters": filt,
        #        "format": "json",
        #        "fields": fields,
        #        "sort": "file_id",
        #        "from": offset,
        #        "size": page_size,
        #    }
        #    page_num = int(offset / gdc_files_page_size) + 1
        #    total_page = int(total_files / gdc_files_page_size) + 1
        #    sys.stderr.write(f"Pulling files page {page_num}/{total_page}\n")
        #    result = retry_get(self.files_endpoint, params=params)
        #    for hit in result.json()["data"]["hits"]:
        #        yield hit

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
                        if self.parent_spec:
                            analyte["files"].extend(aliquot["files"])
                    if self.parent_spec:
                        portion["files"].extend(analyte["files"])
                if self.parent_spec:
                    sample["files"].extend(portion["files"])

        return new_case_record

    def _fetch_file_data_from_cache(self, cache_file: pathlib.PosixPath) -> dict:
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
            # associated_entities = []
            for entity in file_meta.get("associated_entities", []):
                # associated_entities.append(entity['entity_id'])
                files_per_sample_dict[entity["entity_id"]].append(file_meta["file_id"])
            # for case in file_meta.get("cases", []):
            #    for sample in case.get("samples", []):
            #        files_per_sample_dict[sample.get("sample_id")] += [
            #            file_meta.get("file_id")
            #        ]
            #        for portion in sample.get("portions", []):
            #            files_per_sample_dict[portion.get("portion_id")] += [
            #                file_meta.get("file_id")
            #            ]
            #            for slide in portion.get("slides", []):
            #                files_per_sample_dict[slide.get("slide_id")] += [
            #                    file_meta.get("file_id")
            #                ]
            #            for analyte in portion.get("analytes", []):
            #                files_per_sample_dict[analyte.get("analyte_id")] += [
            #                    file_meta.get("file_id")
            #                ]
            #                for aliquot in analyte.get("aliquots", []):
            #                    files_per_sample_dict[aliquot.get("aliquot_id")] += [
            #                        file_meta.get("file_id")
            #                    ]

        return files_per_sample_dict

    def _get_gdc_files(self):
        total_files = get_total_number(self.files_endpoint)

        for offset in range(0, total_files, gdc_files_page_size):
            params = {
                "format": "json",
                "fields": fields,
                "sort": "file_id",
                "from": offset,
                "size": gdc_files_page_size,
            }
            page_num = int(offset / gdc_files_page_size) + 1
            total_page = int(total_files / gdc_files_page_size) + 1
            sys.stderr.write(f"Pulling files page {page_num}/{total_page}\n")
            result = retry_get(self.files_endpoint, params=params)
            for hit in result.json()["data"]["hits"]:
                yield hit

    def prune_specimen_tree(self, file_rec):
        ret = file_rec.copy()
        associations = defaultdict(list)
        if "associated_entities" not in file_rec:
            if "samples" in file_rec:
                file_rec["samples"].pop()
            return file_rec
        for entity in file_rec.get("associated_entities", []):
            if entity["entity_type"] == "case":
                associations["cases"].append(entity["entity_id"])
            else:
                associations["specimens"].append(entity["entity_id"])
        cases = []
        for case in file_rec["cases"]:
            if len(associations["specimens"]) != 0:
                samples = []
                for sample in case.get("samples", []):
                    sample_include = sample["sample_id"] in associations["specimens"]
                    portions = []
                    for portion in sample.get("portions", []):
                        portion_include = (
                            portion["portion_id"] in associations["specimens"]
                        )
                        slides = [
                            slide
                            for slide in portion.get("slides", [])
                            if slide["slide_id"] in associations["specimens"]
                        ]
                        analytes = []
                        for analyte in portion.get("analytes", []):
                            analyte_include = (
                                analyte["analyte_id"] in associations["specimens"]
                            )
                            aliquots = [
                                aliquot
                                for aliquot in analyte.get("aliquots", [])
                                if aliquot["aliquot_id"] in associations["specimens"]
                            ]
                            if len(aliquots) > 0 or analyte_include:
                                analyte["aliquots"] = aliquots
                                analytes.append(analyte)
                        if len(analytes) > 0 or len(slides) > 0 or portion_include:
                            portion["analytes"] = analytes
                            portion["slides"] = slides
                            portions.append(portion)
                    if len(portions) > 0 or sample_include:
                        sample["portions"] = portions
                        samples.append(sample)
                if len(samples) > 0:
                    case["samples"] = samples
            else:
                case["samples"] = []
            cases.append(case)
        ret["cases"] = cases
        return ret


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull case data from GDC API.")
    parser.add_argument("out_file", help="Out file name. Should end with .gz")
    parser.add_argument("fields_list", help="list of fields for endpoint.")
    parser.add_argument("--case", help="Extract just this case")
    parser.add_argument(
        "--cases", help="Optional file with list of case ids (one to a line)"
    )
    parser.add_argument("--file", help="Extract just this file")
    parser.add_argument(
        "--files", help="Optional file with list of file ids (one to a line)"
    )
    
    parser.add_argument(
        "--endpoint", help="Extract all from 'files' or 'cases' endpoint "
    )
    #parser.add_argument(
    #    "--parent_spec", default=True,
    #    help="Add files to parent specimens records writing/using this file.",
    #)
    args = parser.parse_args()
    with open(args.fields_list) as file:
        fields = [line.rstrip() for line in file]
    if len(fields) == 0:
        sys.stderr.write("You done messed up A-A-RON! You need a list of fields")
        return
    gdc = GDC(
        #cache_file=pathlib.Path(args.cache_file), parent_spec=args.parent_spec, 
        field_break=100, fields=fields
    )
    
    if args.case or args.cases or args.endpoint == "cases":
        gdc.save_cases(
            args.out_file,
            case_ids=get_case_ids(case=args.case, case_list_file=args.cases),
        )
    if args.file or args.files or args.endpoint == "files":
        gdc.save_files(
            args.out_file,
            file_ids=get_case_ids(case=args.file, case_list_file=args.files),
        )


if __name__ == "__main__":
    main()
