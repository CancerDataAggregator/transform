import json
from typing import Iterable
import jsonlines
import gzip
import sys
import time
import argparse
from collections import defaultdict
import pathlib

from cdatransform.lib import get_ids
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


def find_all_deepest_specimens(samples) -> list:
    ids = []
    for sample in samples:
        try:
            for portion in sample["portions"]:
                if portion.get("slides") or portion.get("analytes"):
                    ids.extend(
                        [slide["slide_id"] for slide in portion.get("slides", [])]
                    )
                    if portion.get("analytes"):
                        for analyte in portion["analytes"]:
                            try:
                                ids.extend(
                                    [
                                        aliquot["aliquot_id"]
                                        for aliquot in analyte["aliquots"]
                                    ]
                                )
                            except:
                                ids.append(analyte["analyte_id"])
                else:
                    ids.append(portion["portion_id"])
        except:
            ids.append(sample["sample_id"])
    return ids


class GDC:
    def __init__(
        self,
        cases_endpoint: str = "https://api.gdc.cancer.gov/v0/cases",
        files_endpoint: str = "https://api.gdc.cancer.gov/v0/files",
        fields: list = [],
        make_spec_file=None,
    ) -> None:
        self.cases_endpoint = cases_endpoint
        self.files_endpoint = files_endpoint
        self.fields = fields
        self.make_spec_file = make_spec_file

    def det_field_chunks(self, num_chunks) -> list:
        field_groups = defaultdict(list)
        field_chunks = [[] for i in range(num_chunks)]
        for field in self.fields:
            field_groups[field.split(".")[0]].append(field)
        chunk = 0
        for field_group, field_list in field_groups.items():
            field_chunks[chunk].extend(field_list)
            if len(field_chunks[chunk]) > len(self.fields) / num_chunks:
                chunk += 1
        for chunk in field_chunks:
            if "id" not in chunk:
                chunk.append("id")
        return [i for i in field_chunks if len(i) > 1]

    def _paginate_files_or_cases(
        self,
        ids: list = None,
        endpt: str = "case",
        page_size: int = 500,
        num_field_chunks: int = 2,
    ) -> Iterable:
        if ids is not None:
            filt = json.dumps(
                {
                    "op": "and",
                    "content": [
                        {
                            "op": "in",
                            "content": {"field": endpt + "_id", "value": ids},
                        }
                    ],
                }
            )
        else:
            filt = None
        if endpt == "case":
            endpt = self.cases_endpoint
        elif endpt == "file":
            endpt = self.files_endpoint
        offset: int = 0
        field_chunks = self.det_field_chunks(num_field_chunks)
        while True:
            all_hits_dict = defaultdict(list)
            for field_chunk in field_chunks:
                fields = ",".join(field_chunk)
                params = {
                    "filters": filt,
                    "format": "json",
                    "fields": fields,
                    "size": page_size,
                    "from": offset,
                    # "sort": "file_id",
                }
                # sys.stderr.write(str(params))
                result = retry_get(endpt, params=params)
                hits = result.json()["data"]["hits"]
                for hit in hits:
                    all_hits_dict[hit["id"]].append(hit)
                page = result.json()["data"]["pagination"]
                p_no = page.get("page")
                p_tot = page.get("pages")
            sys.stderr.write(f"Pulling page {p_no} / {p_tot}\n")
            res_list = [
                {key: value for record in records for key, value in record.items()}
                for records in all_hits_dict.values()
            ]
            for result in res_list:
                yield result
            if p_no >= p_tot:
                break
            else:
                offset += page_size

    def save_cases(
        self, out_file: str, case_ids: str = None, page_size: int = 500
    ) -> None:
        t0 = time.time()
        n = 0
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            for case in self._paginate_files_or_cases(case_ids, "case", page_size, 3):
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
        # need to write dictionary of file_ids per specimen (specimen: [files])
        specimen_files_dict = defaultdict(list)
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            for file in self._paginate_files_or_cases(
                file_ids, "file", page_size, 2
            ):  # _files(file_ids, page_size):
                writer.write(file)
                n += 1
                if n % page_size == 0:
                    sys.stderr.write(f"Wrote {n} files in {time.time() - t0}s\n")
                if self.make_spec_file:
                    for entity in file.get("associated_entities", []):
                        if entity["entity_type"] != "case":
                            specimen_files_dict[entity["entity_id"]].append(
                                file["file_id"]
                            )

        sys.stderr.write(f"Wrote {n} files in {time.time() - t0}s\n")
        if self.make_spec_file:
            for specimen, files in specimen_files_dict.items():
                specimen_files_dict[specimen] = list(set(files))
            with gzip.open(self.make_spec_file, "wt", encoding="ascii") as out:
                json.dump(specimen_files_dict, out)
                # Portion of code assumes cases, specimens, etc included in File calls (they aren't)
                # if file_ids:
                #    for file in reader:
                #        if file.get("file_id") in file_ids:
                #            writer.write(self.prune_specimen_tree(file))
                #            n += 1
                # else:
                #    for file in reader:
                #        # add parent specimens to list of associated entities. remove cases?
                #        if "associated_entities" not in file:
                #            print(file)
                #        writer.write(self.prune_specimen_tree(file))
                #        n += 1
        # sys.stderr.write(f"Extracted {n} files from cache in {time.time() - t0}s\n")

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
    parser.add_argument(
        "--make_spec_file",
        help="Name of file with files per specimen mapping. If None, don't make it",
    )
    # parser.add_argument(
    #    "--parent_spec", default=True,
    #    help="Add files to parent specimens records writing/using this file.",
    # )
    args = parser.parse_args()
    with open(args.fields_list) as file:
        fields = [line.rstrip() for line in file]
    if len(fields) == 0:
        sys.stderr.write("You done messed up A-A-RON! You need a list of fields")
        return
    gdc = GDC(
        # cache_file=pathlib.Path(args.cache_file), parent_spec=args.parent_spec,
        fields=fields,
        make_spec_file=args.make_spec_file,
    )

    if args.case or args.cases or args.endpoint == "cases":
        gdc.save_cases(
            args.out_file,
            case_ids=get_ids(id=args.case, id_list_file=args.cases),
        )
    if args.file or args.files or args.endpoint == "files":
        gdc.save_files(
            args.out_file,
            file_ids=get_ids(id=args.file, id_list_file=args.files),
        )


if __name__ == "__main__":
    main()
