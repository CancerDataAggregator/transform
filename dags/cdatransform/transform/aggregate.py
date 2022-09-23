import argparse
from collections import defaultdict
import jsonlines
import yaml
import gzip
import logging
from typing import DefaultDict

import cdatransform.transform.merge.merge_functions as mf
import jsonlines
import yaml
from cdatransform.transform.validate import LogValidation

logger = logging.getLogger(__name__)


def get_coalesce_field_names(merge_field_dict) -> list:
    coal_fields: list = [
        key
        for key, val in merge_field_dict.items()
        if val.get("merge_type") == "coalesce"
    ]
    return coal_fields


def prep_log_merge_error(
    entities, merge_field_dict, endpoint
) -> tuple[list, dict, str, str]:
    sources = list(entities.keys())
    coal_fields: list = get_coalesce_field_names(merge_field_dict)
    ret_dat: dict = {}
    for source in sources:
        temp: dict = {field: entities.get(source).get(field) for field in coal_fields}
        ret_dat[source] = temp
    for source, val in entities.items():
        if val.get("id") is not None:
            id: str = val["id"]
            break
    for source, val in entities.items():
        if endpoint == "subjects":
            project: str = val.get("ResearchSubject")[0].get(
                "member_of_research_project"
            )
            break
        else:
            project: str = val.get("associated_project", "from-pdc-metadata-query")
            break
    return coal_fields, ret_dat, id, project


def log_merge_error(entities, all_sources, fields, log, endpoint):
    coal_fields, coal_dat, id, project = prep_log_merge_error(
        entities, fields, endpoint
    )
    all_sources.insert(
        0, endpoint[:-1]
    )  # remove the 's' at the end of the endpoint names
    all_sources.insert(1, id)
    all_sources.insert(2, "project")
    all_sources.insert(3, project)
    log.agree_sources(coal_dat, "_".join(all_sources), coal_fields)
    return log


def main(debug=False):
    parser = argparse.ArgumentParser(
        description="Aggregate data from DC to Subject/File level."
    )
    parser.add_argument(
        "merge_file", help="file name for how to merge fields. Should end with .yml"
    )
    parser.add_argument(
        "input_file",
        help="Input file name. Should be output file of transform function. Should end with .gz",
    )
    parser.add_argument("output_file", help="Out file name. Should end with .gz")
    parser.add_argument("--log", default="aggregate.log", help="Name of log file.")
    parser.add_argument(
        "--endpoint",
        default="subjects",
        help="endpoint you are aggregating. files or subjects",
    )
    args = parser.parse_args()
    logging.basicConfig(
        filename=args.log,
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
    )
    logger.info("----------------------")
    logger.info("Starting aggregate run")
    logger.info("----------------------")

    endpoint = args.endpoint
    with open(args.merge_file) as file:
        how_to_merge = yaml.full_load(file)
    with gzip.open(args.input_file, "r") as infp:
        readDC = jsonlines.Reader(infp)
        entity_rec_mapping = defaultdict(list)
        for rec in readDC:
            id = rec["id"]
            entity_rec_mapping[id].append(rec)
    with gzip.open(args.output_file, "w") as outfp:
        writeDC = jsonlines.Writer(outfp)
        log = LogValidation()
        for id, records in entity_rec_mapping.items():
            if len(records) == 1:
                merged_entry = records[0]
            else:
                entities = {k: patient for k, patient in enumerate(records)}
                lines_ids = list(range(len(records)))
                if endpoint == "subjects":
                    merged_entry = mf.merge_fields_level(
                        entities, how_to_merge["Patient_merge"], lines_ids
                    )
                    case_ids = [
                        record.get("ResearchSubject")[0].get("id") for record in records
                    ]
                    log = log_merge_error(
                        entities, case_ids, how_to_merge["Patient_merge"], log, endpoint
                    )

                else:  # Assume files endpoint
                    merged_entry = mf.merge_fields_level(
                        entities, how_to_merge["File_merge"], lines_ids
                    )
                    file_ids = [record.get("id") for record in records]
                    log = log_merge_error(
                        entities, file_ids, how_to_merge["File_merge"], log, endpoint
                    )

            writeDC.write(merged_entry)
        log.generate_report(logging.getLogger("test"))


if __name__ == "__main__":
    main()
