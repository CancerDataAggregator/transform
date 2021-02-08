import json
import jsonlines
import time
import sys
import gzip
import argparse
from itertools import groupby

from cdatransform.lib import get_case_ids
from .lib import retry_get
from .pdc_query_lib import query_all_cases, query_single_case, query_files_bulk


class PDC:
    def __init__(self, endpoint="https://pdc.cancer.gov/graphql") -> None:
        self.endpoint = endpoint

    def cases(
        self,
        case_ids=None,
    ):

        if case_ids is None:
            case_ids = self.get_case_id_list()

        for case_id in case_ids:
            result = retry_get(self.endpoint, params={"query": query_single_case(case_id=case_id)})
            yield result.json()["data"]["case"][0]

    def get_case_id_list(self):
        result = retry_get(self.endpoint, params={"query": query_all_cases()})
        for case in result.json()["data"]["allCases"]:
            yield case["case_id"]

    def files_chunk(self):
        for page in range(0, 90000, 1000):
            sys.stderr.write(f"<< Processing page {int(page/1000) + 1}/90 >>\n")
            result = retry_get(self.endpoint, params={"query": query_files_bulk(page, 1000)})
            yield result.json()["data"]["fileMetadata"]

    def get_files_per_sample(self) -> dict:
        t0 = time.time()
        n = 0
        sample_file_pairs = []
        files_per_sample = dict()
        sys.stderr.write(f"Started collecting files.\n")

        for fc in self.files_chunk():
            for f in fc:
                aliquots = f.get("aliquots")
                file_metadata = get_file_metadata(f)
                if aliquots:
                    for aliquot in aliquots:
                        sample_id = aliquot["sample_id"]
                        sample_file_pairs.append({"sample_id": sample_id, "file_metadata": file_metadata})
                        n += 1
            sys.stderr.write(f"Chunk completed. Wrote {n} sample-file pairs in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} sample-file pairs in {time.time() - t0}s\n")

        t1 = time.time()
        sample_file_pairs.sort(key=lambda x: x["sample_id"])
        for sample_id, all_sample_pairs in groupby(sample_file_pairs, key=lambda x: x["sample_id"]):
            all_files_per_sample = [pair["file_metadata"] for pair in all_sample_pairs]
            files_per_sample[sample_id] = all_files_per_sample
        sys.stderr.write(f"Created a files look-up dict for {len(files_per_sample)} samples in {time.time() - t1}s\n")
        sys.stderr.write(f"Entire files preparation completed in {time.time() - t0}s\n\n")
        return files_per_sample

    def save_cases(self, out_file, case_ids=None):
        t0 = time.time()
        n = 0
        files_per_sample = self.get_files_per_sample()
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            for case in self.cases(case_ids):
                for index, sample in enumerate(case["samples"]):
                    case["samples"][index]["File"] = files_per_sample.get(sample["sample_id"])
                writer.write(case)
                n += 1
                if n % 100 == 0:
                    sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")


def get_file_metadata(file_metadata_record) -> dict:
    file_metadata = dict()
    file_metadata["file_id"] = file_metadata_record.get("file_id")
    file_metadata["file_name"] = file_metadata_record.get("file_name")
    file_metadata["file_location"] = file_metadata_record.get("file_location")
    file_metadata["file_submitter_id"] = file_metadata_record.get("file_submitter_id")
    file_metadata["file_type"] = file_metadata_record.get("file_type")
    file_metadata["file_format"] = file_metadata_record.get("file_format")
    file_metadata["file_size"] = file_metadata_record.get("file_size")
    file_metadata["data_category"] = file_metadata_record.get("data_category")
    file_metadata["experiment_type"] = file_metadata_record.get("experiment_type")
    file_metadata["md5sum"] = file_metadata_record.get("md5sum")
    return file_metadata


def main():
    parser = argparse.ArgumentParser(description="Pull case data from PDC API.")
    parser.add_argument("out_file", help="Out file name. Should end with .gz")
    parser.add_argument("--case", help="Extract just this case")
    parser.add_argument(
        "--cases", help="Optional file with list of case ids (one to a line)"
    )
    args = parser.parse_args()

    pdc = PDC()
    pdc.save_cases(
        args.out_file, case_ids=get_case_ids(case=args.case, case_list_file=args.cases)
    )


if __name__ == "__main__":
    main()
