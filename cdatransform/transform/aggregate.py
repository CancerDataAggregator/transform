import argparse
from typing import DefaultDict
import jsonlines
import yaml
import gzip
import logging
import cdatransform.transform.merge.merge_functions as mf
from cdatransform.transform.validate import LogValidation

logger = logging.getLogger(__name__)


def get_coalesce_field_names(merge_field_dict):
    coal_fields = []
    for key, val in merge_field_dict.items():
        if val.get("merge_type") == "coalesce":
            coal_fields.append(key)
    return coal_fields


def prep_log_merge_error(entities, merge_field_dict):
    sources = list(entities.keys())
    coal_fields = get_coalesce_field_names(merge_field_dict)
    ret_dat = dict()
    for source in sources:
        temp = dict()
        for field in coal_fields:
            temp[field] = entities.get(source).get(field)
        ret_dat[source] = temp
    for source, val in entities.items():
        if val.get("id") is not None:
            patient_id = val.get("id")
            break
    for source, val in entities.items():
        if val.get("ResearchSubject")[0].get("member_of_research_project") is not None:
            project = val.get("ResearchSubject")[0].get("member_of_research_project")
            break
    return coal_fields, ret_dat, patient_id, project


def log_merge_error(entities, all_sources, fields, log):
    coal_fields, coal_dat, patient_id, project = prep_log_merge_error(entities, fields)
    all_sources.insert(0, "patient")
    all_sources.insert(1, patient_id)
    all_sources.insert(2, "project")
    all_sources.insert(3, project)
    log.agree_sources(coal_dat, "_".join(all_sources), coal_fields)
    return log


def merge_entities_with_same_id(entity_recs, how_to_merge_entity):
    entities = DefaultDict(list)
    rec = []
    for entity in entity_recs:
        id = entity.get("id")
        entities[id] += [entity]
    for id, recs in entities.items():
        if len(recs) == 1:
            rec += recs
        else:
            entities = {k: case for k, case in enumerate(recs)}
            lines_recs = list(range(len(recs)))
            rec += [mf.merge_fields_level(entities, how_to_merge_entity, lines_recs)]
            # case_ids = [patient.get('ResearchSubject')[0].get('id') for patient in patients]
            # log = log_merge_error(entities, case_ids, how_to_merge["Patient_merge"], log)
    return rec


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
    parser.add_argument("--log", default="aggregate.log", help="Name of log file.")
    args = parser.parse_args()
    logging.basicConfig(
        filename=args.log,
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
    )
    logger.info("----------------------")
    logger.info("Starting aggregate run")
    logger.info("----------------------")
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
        log = LogValidation()
        for patient_id, patients in patient_case_mapping.items():
            if len(patients) == 1:
                merged_entry = patients[0]
            else:
                entities = {k: patient for k, patient in enumerate(patients)}
                lines_cases = list(range(len(patients)))
                merged_entry = mf.merge_fields_level(
                    entities, how_to_merge["Patient_merge"], lines_cases
                )
                case_ids = [
                    patient.get("ResearchSubject")[0].get("id") for patient in patients
                ]
                # file_ids = [file.get('id') for patient in patients for file in patient.get('File')]
                log = log_merge_error(
                    entities, case_ids, how_to_merge["Patient_merge"], log
                )

            #merged_entry["File"] = merge_entities_with_same_id(
            #    merged_entry["File"], how_to_merge["File_merge"]
            #)
            merged_entry["ResearchSubject"] = merge_entities_with_same_id(
                merged_entry["ResearchSubject"], how_to_merge["ResearchSubject_merge"]
            )
            for RS in merged_entry["ResearchSubject"]:
                #RS["File"] = merge_entities_with_same_id(
                #    RS["File"], how_to_merge["File_merge"]
                #)
                RS["Diagnosis"] = merge_entities_with_same_id(
                    RS["Diagnosis"], how_to_merge["Diagnosis_merge"]
                )
                RS["Specimen"] = merge_entities_with_same_id(
                    RS["Specimen"], how_to_merge["Specimen_merge"]
                )
                #for specimen in RS["Specimen"]:
                #    specimen["File"] = merge_entities_with_same_id(
                #        specimen["File"], how_to_merge["File_merge"]
                #    )
            writeDC.write(merged_entry)
        log.generate_report(logging.getLogger("test"))


if __name__ == "__main__":
    main()
