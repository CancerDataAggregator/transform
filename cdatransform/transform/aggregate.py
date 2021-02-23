import argparse
from typing import DefaultDict
import jsonlines
import yaml
import gzip

import cdatransform.transform.merge.merge_functions as mf


def main():
    parser = argparse.ArgumentParser(
        description="Aggregate cases data from DC to Patient level."
    )
    parser.add_argument(
        "merge_file", help="file name for how to merge fields. Should end with .yml"
    )
    parser.add_argument(
        "input_file",
        help="Input file name. Should be output file of transform function. Should end with .gz",
    )
    parser.add_argument("output_file", help="Out file name. Should end with .gz")
    args = parser.parse_args()
    # yaml.load(open(transform_file, "r"), Loader=Loader)
    with open(args.merge_file) as file:
        how_to_merge = yaml.full_load(file)
    with gzip.open(args.input_file, "r") as infp:
        readDC = jsonlines.Reader(infp)
        patient_case_mapping = DefaultDict(list)
        for case in readDC:
            patient_id = case.get("id")
            patient_case_mapping[patient_id] += [case]

    with gzip.open(args.output_file, "w") as outfp:
        writeDC = jsonlines.Writer(outfp)
        for patient_id, patients in patient_case_mapping.items():
            if len(patients) == 1:
                writeDC.write(patients[0])
            else:
                entities = {k: patient for k, patient in enumerate(patients)}
                lines_cases = list(range(len(patients)))

                merged_entry = mf.merge_fields_level(
                    entities, how_to_merge["Patient_merge"], lines_cases
                )
                writeDC.write(merged_entry)


if __name__ == "__main__":
    main()
