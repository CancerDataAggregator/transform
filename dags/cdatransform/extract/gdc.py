import argparse
import gzip
import json
import pathlib
import sys
import time
from collections import defaultdict
from typing import Iterable

import jsonlines

from ..lib import get_case_ids
from .gdc_case_fields import case_fields
from .gdc_file_fields import file_fields
from .lib import retry_get

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
        # parent_spec: bool = True,
        field_break: int = 103,
        fields: list = [],
        make_spec_file=None,
    ) -> None:
        self.cases_endpoint = cases_endpoint
        self.files_endpoint = files_endpoint
        self.field_break = field_break
        self.fields = fields
        self.make_spec_file = make_spec_file

    def det_field_chunks(self, num_chunks) -> list:
        field_groups = defaultdict(list)
        field_chunks = [[] for _ in range(num_chunks)]
        for field in self.fields:
            field_groups[field.split(".")[0]].append(field)
        chunk = 0
        for _, field_list in field_groups.items():
            field_chunks[chunk].extend(field_list)
            if len(field_chunks[chunk]) > len(self.fields) / num_chunks:
                chunk += 1
        for chunk_array in field_chunks:
            if "id" not in chunk_array:
                chunk_array.append("id")
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
                # print(str(params))
                result = retry_get(endpt, params=params)
                hits = result.json()["data"]["hits"]
                for hit in hits:
                    all_hits_dict[hit["id"]].append(hit)
                page = result.json()["data"]["pagination"]
                current_page = page.get("page")
                current_pages = page.get("pages")

                print(f"Pulling page {current_page} / {current_pages}\n", flush=True)
                print(all_hits_dict)
                res_list = [
                    {key: value for record in records for key, value in record.items()}
                    for records in all_hits_dict.values()
                ]
                for result in res_list:
                    yield result
                if current_page >= current_pages:
                    break
                else:
                    offset += page_size

    def save_cases(
        self, out_file: str, case_ids: str = None, page_size: int = 500
    ) -> None:
        self.fields = case_fields
        print("starting save cases")
        t0 = time.time()
        n = 0
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            for case in self._paginate_files_or_cases(case_ids, "case", page_size, 3):
                writer.write(case)
                n += 1
                if n % page_size == 0:
                    print(f"Wrote {n} cases in {time.time() - t0}s\n", flush=True)
        print(f"Wrote {n} cases in {time.time() - t0}s\n", flush=True)

    def save_files(
        self, out_file: str, file_ids: list = None, page_size: int = 5000
    ) -> None:
        self.fields = file_fields
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
                    print(f"Wrote {n} files in {time.time() - t0}s\n", flush=True)
                if self.make_spec_file:
                    for entity in file.get("associated_entities", []):
                        if entity["entity_type"] != "case":
                            specimen_files_dict[entity["entity_id"]].append(
                                file["file_id"]
                            )

        print(f"Wrote {n} files in {time.time() - t0}s\n", flush=True)
        if self.make_spec_file:
            for specimen, files in specimen_files_dict.items():
                specimen_files_dict[specimen] = list(set(files))
            with gzip.open(self.make_spec_file, "wt", encoding="ascii") as out:
                json.dump(specimen_files_dict, out)


# def main() -> None:


#     # gdc.save_cases(
#     #         args.out_file,
#     #         case_ids=get_case_ids(case=args.case, case_list_file=args.cases),
#     #     )
#     # gdc.save_files(
#     #         args.out_file,
#     #         file_ids=get_case_ids(case=args.file, case_list_file=args.files),
#     #     )


# if __name__ == "__main__":
#     main()
