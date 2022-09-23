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

class GDC:
    def __init__(
        self,
        cases_endpoint: str = "https://api.gdc.cancer.gov/v0/cases",
        files_endpoint: str = "https://api.gdc.cancer.gov/v0/files",
        fields: list[str] = [],
        make_spec_file: str = None,
    ) -> None:
        self.cases_endpoint = cases_endpoint
        self.files_endpoint = files_endpoint
        self.fields = fields
        self.make_spec_file = make_spec_file

    def det_field_chunks(self, num_chunks: int) -> list[list[str]]:
        """This code uses a txt file with a list of fields to pull from GDC case/file
        endpoints. Unfortunately GDC API does not let you use all of them in one query.
        This code (usually) breaks up this list of fields into num_chunks number of 
        chunks. Code will later query using fields from chunk 1, chunk 2, etc, and merge 
        it all together

        Args:
            num_chunks (int): number of chunks you want to attempt to bring the field list into

        Returns:
            list: returns list of lists [ [field1, field2], [field3, field4]...]
        """
        field_groups = defaultdict(list)
        field_chunks: list[list[str]] = [[] for i in range(num_chunks)]
        for field in self.fields:
            field_groups[field.split(".")[0]].append(field)
        chunk = 0
        for field_list in field_groups.values():
            field_chunks[chunk].extend(field_list)
            if len(field_chunks[chunk]) > len(self.fields) / num_chunks:
                chunk += 1
        for chunk in field_chunks:
            if "id" not in chunk:
                chunk.append("id")
        return [i for i in field_chunks if len(i) > 1]

    def _paginate_files_or_cases(
        self,
        ids: list[str]|None = None,
        endpt: str = "case",
        page_size: int = 500,
        num_field_chunks: int = 2,
    ) -> Iterable[dict[str,str|int|list]]:
        """Gets page of data, returns each hit one by one, gets next page, etc.

        Args:
            ids (list, optional): List of case or file ids. Defaults to None. If
                None, then doing bulk download
            endpt (str, optional): used to build filter in query and determine endpoint
                to query. Either case_id or file_id. Defaults to "case".
            page_size (int, optional): Number or results in paginated return from the API. 
                Defaults to 500.
            num_field_chunks (int, optional): List of fields pulled must be broken into chunks 
                since the query can only be so long. This is the number of chunks you are attempting 
                to break it into. Defaults to 2.

        Returns:
            Iterable: _description_

        Yields:
            Iterator[Iterable]: 
        """
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
            all_hits_dict = defaultdict(list) # { id1: [{record1 - fields from chunk1}, 
                                              #         {record2 - fields from chunk 2}]}
            for field_chunk in field_chunks:
                fields = ",".join(field_chunk)
                params = {
                    "filters": filt,
                    "format": "json",
                    "fields": fields,
                    "size": page_size,
                    "from": offset,
                }
                result = retry_get(endpt, params=params)
                hits = result.json()["data"]["hits"]
                for hit in hits:
                    all_hits_dict[hit["id"]].append(hit)
                page = result.json()["data"]["pagination"]
                p_no = page.get("page")
                p_tot = page.get("pages")
            sys.stderr.write(f"Pulling page {p_no} / {p_tot}\n")
            # Merge records of same case/file so there is one record with all desired fields
            res_list = [
                {key: value for record in records for key, value in record.items()}
                for records in all_hits_dict.values()
            ]
            # return each record as a result
            for result in res_list:
                yield result
            if p_no >= p_tot:
                break
            else:
                offset += page_size

    def save_cases(
        self, out_file: str, case_ids: list[str]|None = None, page_size: int = 500
    ) -> None:
        t0:float = time.time()
        n:int = 0
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            for case in self._paginate_files_or_cases(case_ids, "case", page_size, 3):
                writer.write(case)
                n += 1
                if n % page_size == 0:
                    sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")

    def save_files(
        self, out_file: str, file_ids: list[str]|None = None, page_size: int = 5000
    ) -> None:
        t0:float = time.time()
        n:int = 0
        # need to write dictionary of file_ids per specimen (specimen: [files])
        specimen_files_dict:defaultdict[str,list[str]] = defaultdict(list)
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

    args = parser.parse_args()
    with open(args.fields_list) as file:
        fields = [line.rstrip() for line in file]
    if len(fields) == 0:
        sys.stderr.write("You done messed up A-A-RON! You need a list of fields")
        return
    gdc = GDC(
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
