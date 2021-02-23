import json
from collections import defaultdict

import jsonlines
import time
import sys
import gzip
import argparse
import pathlib

from cdatransform.lib import get_case_ids
from .lib import retry_get
from .pdc_query_lib import query_all_cases, query_single_case, query_files_bulk


class PDC:
    def __init__(self, cache_file, endpoint="https://pdc.cancer.gov/graphql") -> None:
        self.endpoint = endpoint
        self._files_per_sample_dict = self._fetch_file_data_from_cache(cache_file)

    def cases(
        self,
        case_ids=None,
    ):

        if case_ids is None:
            case_ids = self.get_case_id_list()

        for case_id in case_ids:
            result = retry_get(
                self.endpoint, params={"query": query_single_case(case_id=case_id)}
            )
            yield result.json()["data"]["case"][0]

    def get_case_id_list(self):
        result = retry_get(self.endpoint, params={"query": query_all_cases()})
        for case in result.json()["data"]["allCases"]:
            yield case["case_id"]

    def save_cases(self, out_file, case_ids=None):
        t0 = time.time()
        n = 0
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            for case in self.cases(case_ids):
                for index, sample in enumerate(case["samples"]):
                    case["samples"][index]["File"] = self._files_per_sample_dict.get(
                        sample["sample_id"]
                    )
                writer.write(case)
                n += 1
                if n % 100 == 0:
                    sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")

    def _fetch_file_data_from_cache(self, cache_file):
        if not cache_file.exists():
            sys.stderr.write(f"Cache file {cache_file} not found. Generating.\n")
            files_per_sample_dict = self._get_files_per_sample_dict()
            with gzip.open(cache_file, "w") as f_out:
                json.dump(files_per_sample_dict, f_out)
        else:
            sys.stderr.write(f"Loading files metadata from cache file {cache_file}.\n")
            with gzip.open(cache_file, "r") as f_in:
                files_per_sample_dict = json.load(f_in)
        return files_per_sample_dict

    def _get_files_per_sample_dict(self) -> dict:
        t0 = time.time()
        n = 0
        files_per_sample = defaultdict(list)
        sys.stderr.write("Started collecting files.\n")

        for fc in self._files_chunk():
            for f in fc:
                aliquots = f.get("aliquots")
                file_metadata = get_file_metadata(f)
                if aliquots:
                    for aliquot in aliquots:
                        files_per_sample[aliquot["sample_id"]].append(file_metadata)
                        n += 1
            sys.stderr.write(
                f"Chunk completed. Wrote {n} sample-file pairs in {time.time() - t0}s\n"
            )
        sys.stderr.write(f"Wrote {n} sample-file pairs in {time.time() - t0}s\n")

        t1 = time.time()
        sys.stderr.write(
            f"Created a files look-up dict for {len(files_per_sample)} samples in {time.time() - t1}s\n"
        )
        sys.stderr.write(
            f"Entire files preparation completed in {time.time() - t0}s\n\n"
        )
        return files_per_sample

    def _files_chunk(self):
        for page in range(0, 90000, 1000):
            sys.stderr.write(f"<< Processing page {int(page/1000) + 1}/90 >>\n")
            result = retry_get(
                self.endpoint, params={"query": query_files_bulk(page, 1000)}
            )
            yield result.json()["data"]["fileMetadata"]


def get_file_metadata(file_metadata_record) -> dict:
    return {
        field: file_metadata_record.get(field)
        for field in [
            "file_id",
            "file_name",
            "file_location",
            "file_submitter_id",
            "file_type",
            "file_format",
            "file_size",
            "data_category",
            "experiment_type",
            "md5sum",
        ]
    }


def main():
    parser = argparse.ArgumentParser(description="Pull case data from PDC API.")
    parser.add_argument("out_file", help="Out file name. Should end with .gz")
    parser.add_argument("cache_file", help="Use (or generate if missing) cache file.")
    parser.add_argument("--case", help="Extract just this case")
    parser.add_argument(
        "--cases", help="Optional file with list of case ids (one to a line)"
    )
    args = parser.parse_args()

    pdc = PDC(pathlib.Path(args.cache_file))
    pdc.save_cases(
        args.out_file, case_ids=get_case_ids(case=args.case, case_list_file=args.cases)
    )


if __name__ == "__main__":
    main()
