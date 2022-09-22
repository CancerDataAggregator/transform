import argparse
import gzip
import logging
import sys
from typing import DefaultDict

import cdatransform.transform.merge.merge_functions as mf
import jsonlines
import yaml
from cdatransform.transform.validate import LogValidation

logger = logging.getLogger(__name__)


def split_subjects_endpoint(merged_entities_file, mapping):
    subjects = DefaultDict(list)
    researchsubjects = DefaultDict(list)
    specimens = DefaultDict(list)
    diagnoses = DefaultDict(list)
    treatments = DefaultDict(list)
    # record entities in separate lists, with linker columns
    with gzip.open(merged_entities_file, "r") as infp:
        reader = jsonlines.Reader(infp)
        for subject in reader:
            subject_entity = initialize_entity(subject, mapping, "Subject")
            subject_entity["ResearchSubjects"] = []
            subject_entity["Specimens"] = []
            subject_entity["Diagnoses"] = []
            subject_entity["Treatments"] = []
            subject_entity["Files"] = subject["Files"]
            if "ResearchSubject" in subject:
                for researchsubject in subject["ResearchSubject"]:
                    researchsubject_entity = initialize_entity(
                        researchsubject, mapping, "ResearchSubject"
                    )
                    researchsubject_entity["Subjects"] = [subject["id"]]
                    researchsubject_entity["Specimens"] = []
                    researchsubject_entity["Diagnoses"] = []
                    researchsubject_entity["Treatments"] = []
                    researchsubject_entity["Files"] = researchsubject["Files"]
                    if "Specimen" in researchsubject:
                        for specimen in researchsubject["Specimen"]:
                            specimen_entity = initialize_entity(
                                specimen, mapping, "Specimen"
                            )
                            specimen_entity["Subjects"] = [subject["id"]]
                            specimen_entity["ResearchSubjects"] = [
                                researchsubject["id"]
                            ]
                            specimen_entity["Files"] = specimen["Files"]
                            specimens[specimen_entity["id"]].append(specimen_entity)
                            subject_entity["Specimens"].append(specimen["id"])
                            researchsubject_entity["Specimens"].append(specimen["id"])
                    if "Diagnosis" in researchsubject:
                        for diagnosis in researchsubject["Diagnosis"]:
                            diagnosis_entity = initialize_entity(
                                diagnosis, mapping, "Diagnosis"
                            )
                            diagnosis_entity["Subjects"] = [subject["id"]]
                            diagnosis_entity["ResearchSubjects"] = [
                                researchsubject["id"]
                            ]
                            diagnosis_entity["Treatments"] = []
                            if "Treatment" in diagnosis:
                                for treatment in diagnosis["Treatment"]:
                                    treatment_entity = initialize_entity(
                                        treatment, mapping, "Treatment"
                                    )
                                    treatment_entity["Subjects"] = [subject["id"]]
                                    treatment_entity["ResearchSubjects"] = [
                                        researchsubject["id"]
                                    ]
                                    treatment_entity["Diagnoses"] = [diagnosis["id"]]
                                    treatments[treatment_entity["id"]].append(
                                        treatment_entity
                                    )
                                    diagnosis_entity["Treatments"].append(
                                        treatment_entity["id"]
                                    )
                                    researchsubject_entity["Treatments"].append(
                                        treatment_entity["id"]
                                    )
                                    subject_entity["Treatments"].append(
                                        treatment_entity["id"]
                                    )
                            diagnoses[diagnosis_entity["id"]].append(diagnosis_entity)
                            subject_entity["Diagnoses"].append(diagnosis["id"])
                            researchsubject_entity["Diagnoses"].append(diagnosis["id"])
                    researchsubjects[researchsubject_entity["id"]].append(
                        researchsubject_entity
                    )
                    subject_entity["ResearchSubjects"].append(
                        researchsubject_entity["id"]
                    )
            subjects[subject_entity["id"]].append(subject_entity)
    entities = {
        "Subject": subjects,
        "ResearchSubject": researchsubjects,
        "Specimen": specimens,
        "Diagnosis": diagnoses,
        "Treatment": treatments,
    }
    return entities


def split_files_endpoint(merged_entities_file, mapping):
    # record entities in separate lists, with linker columns
    with gzip.open(merged_entities_file, "r") as infp:
        reader = jsonlines.Reader(infp)
        with gzip.open("File.jsonl.gz", "w") as outfp:
            writeDC = jsonlines.Writer(outfp)
            for file in reader:
                file_entity = initialize_entity(file, mapping, "File")
                file_entity["ResearchSubjects"] = []
                file_entity["Specimens"] = []
                file_entity["Subjects"] = []
                if "ResearchSubject" in file:
                    for researchsubject in file["ResearchSubject"]:
                        file_entity["ResearchSubjects"].append(researchsubject["id"])
                if "Subject" in file:
                    for subject in file["Subject"]:
                        file_entity["Subjects"].append(subject["id"])
                if "Specimen" in file:
                    for specimen in file["Specimen"]:
                        file_entity["Specimens"].append(specimen["id"])
                writeDC.write(file_entity)


def initialize_entity(record, mapping, entity_name):
    entity = {}
    for field in mapping[entity_name + "_merge"]:
        if record.get(field) is not None:
            entity[field] = record.get(field)
    return entity


def aggregate_duplicate_entities(entities_dict, how_to_merge):
    for entity_name, recs in entities_dict.items():
        with gzip.open(entity_name + ".jsonl.gz", "w") as outfp:
            writeDC = jsonlines.Writer(outfp)
            for id, records in recs.items():
                if len(records) == 1:
                    merged_entry = records[0]
                else:
                    entities = {k: id for k, id in enumerate(records)}
                    lines_cases = list(range(len(records)))
                    merged_entry = mf.merge_fields_level(
                        entities, how_to_merge[entity_name + "_merge"], lines_cases
                    )
                writeDC.write(merged_entry)


def main():
    parser = argparse.ArgumentParser(description="Merge data between DCs")
    parser.add_argument(
        "merged_entities_file", help="Name of merged entities file.Should end with .gz"
    )
    parser.add_argument("endpoint", help="Subjects or Files")
    parser.add_argument(
        "merge_file", help="Mapping file name. Contains entity information"
    )

    args = parser.parse_args()
    # logging.basicConfig(
    #    filename=args.log,
    #    format="%(asctime)s %(levelname)-8s %(message)s",
    #    level=logging.INFO,
    # )
    # logger.info("----------------------")
    ##logger.info("Starting merge run")
    # logger.info("----------------------")
    with open(args.merge_file) as file:
        how_to_merge = yaml.full_load(file)

    if args.endpoint == "Subjects":
        entities_dict = split_subjects_endpoint(args.merged_entities_file, how_to_merge)
        aggregate_duplicate_entities(entities_dict, how_to_merge)
    if args.endpoint == "Files":
        entities_dict = split_files_endpoint(args.merged_entities_file, how_to_merge)


if __name__ == "__main__":
    main()
