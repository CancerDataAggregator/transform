import jsonlines
import json
import gzip
import sys
import time
import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description="Add file_ids to GDC Specimens")
    parser.add_argument("cases_file", help="Input cases file name. Should end with .gz")
    parser.add_argument(
        "spec_files_file", help="Input specimen-files file name. Should end with .gz"
    )
    parser.add_argument("out_file", help="Output file name. Should end with .gz")
    parser.add_argument("DC", help="DC you are adding files to - GDC, PDC")

    args = parser.parse_args()
    # Read in specimen:files mapping to dict
    with gzip.open(args.spec_files_file, "r") as s_f_map:
        spec_files = json.loads(s_f_map.read())
    sys.stderr.write(f"Read {len(spec_files)} specimen: file mappings\n")
    # Add list of file_ids to all specimens (samples, portions, etc.)
    t0 = time.time()
    n = 0
    page_size = 10000
    with gzip.open(args.out_file, "wb") as outf:
        writer = jsonlines.Writer(outf)
        with gzip.open(args.cases_file, "r") as inf:
            reader = jsonlines.Reader(inf)
            if args.DC == "GDC":
                for case in reader:
                    out_case = case.copy()
                    out_case["files"] = [i["file_id"] for i in case["files"]]
                    for sample in out_case.get("samples", []):
                        sample["files"] = spec_files.get(sample["sample_id"], [])
                        for portion in sample.get("portions", []):
                            portion["files"] = spec_files.get(portion["portion_id"], [])
                            for slide in portion.get("slides", []):
                                slide["files"] = spec_files.get(slide["slide_id"], [])
                            for analyte in portion.get("analytes", []):
                                analyte["files"] = spec_files.get(
                                    analyte["analyte_id"], []
                                )
                                for aliquot in analyte.get("aliquots", []):
                                    aliquot["files"] = spec_files.get(
                                        aliquot["aliquot_id"], []
                                    )
                    # Added files to all specimens in spec:file dictionary for that case. Write case
                    writer.write(out_case)
                    n += 1
                    if n % page_size == 0:
                        sys.stderr.write(
                            f"Wrote {n} cases w/ spec files {time.time() - t0}s\n"
                        )
            elif args.DC == "PDC":
                for case in reader:
                    out_case = case.copy()
                    out_case["files"] = []
                    for sample in out_case.get("samples", []):
                        sample["files"] = spec_files.get(sample["sample_id"], [])
                        out_case["files"].extend(sample["files"])
                        for aliquot in sample.get("aliquots", []):
                            aliquot["files"] = spec_files.get(aliquot["aliquot_id"], [])
                    # Added files to all specimens in spec:file dictionary for that case. Write case
                    out_case["files"] = list(set(out_case["files"]))
                    writer.write(out_case)
                    n += 1
                    if n % page_size == 0:
                        sys.stderr.write(
                            f"Wrote {n} cases w/ spec files {time.time() - t0}s\n"
                        )

            sys.stderr.write(f"Wrote {n} cases w/ spec files in {time.time() - t0}s\n")


if __name__ == "__main__":
    main()
