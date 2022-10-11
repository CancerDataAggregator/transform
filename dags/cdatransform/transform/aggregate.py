import argparse
from collections import defaultdict
from typing import List, Union, Optional
from typing_extensions import Literal
import jsonlines
import yaml
import gzip
import logging
from typing import DefaultDict
from .merge.merge_functions import merge_fields_level
import jsonlines
import yaml
from .validate import LogValidation
import sys
from .yaml_merge_types import YamlFileMerge

logger = logging.getLogger(__name__)


"""
Custom Types defintions 
"""
Endpoint_type_aggregation = Union[Literal["subjects"], Literal["files"]]


def get_coalesce_field_names(merge_field_dict) -> list[str]:
    coal_fields: list[str] = [
        key
        for key, val in merge_field_dict.items()
        if val.get("merge_type") == "coalesce"
    ]
    return coal_fields


def prep_log_merge_error(
    entities: dict[str, dict], merge_field_dict, endpoint
) -> tuple[list[str], dict, str, str]:
    sources = list(entities.keys())
    coal_fields: list[str] = get_coalesce_field_names(merge_field_dict)
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
            if val.get("associated_project") is None:
                project: Literal["from-metadata-query"] = "from-metadata-query"
            else:
                project = val["associated_project"]
            # project:str = val.get("associated_project", "from-pdc-metadata-query")
            break
    return coal_fields, ret_dat, id, project


def log_merge_error(
    entities: dict,
    all_sources: list,
    fields: dict,
    log,
    endpoint: Endpoint_type_aggregation,
):
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


def merge_entities_with_same_id(entity_recs: list[dict], how_to_merge_entity: dict):
    entities: DefaultDict = defaultdict(list)
    rec: list = []
    for entity in entity_recs:
        id = entity.get("id")
        entities[id] += [entity]
    for id, recs in entities.items():
        if len(recs) == 1:
            rec += recs
        else:
            entities = {k: case for k, case in enumerate(recs)}
            lines_recs = list(range(len(recs)))
            rec += [merge_fields_level(entities, how_to_merge_entity, lines_recs)]
            # case_ids = [patient.get('ResearchSubject')[0].get('id') for patient in patients]
            # log = log_merge_error(entities, case_ids, how_to_merge["Patient_merge"], log)
    return rec


def aggregation(
    input_file: str = "",
    output_file: str = "",
    merge_file: Optional[YamlFileMerge] = None,
    endpoint: Endpoint_type_aggregation = "subjects",
) -> None:
    """_summary_

    Args:
        input_file (str, optional): Input file name. Should be output file of transform function. Should end with .gz. Defaults to "".
        output_file (str, optional): Out file name. Should end with .gz. Defaults to "".
        merge_file (Optional[YamlFileMapping], optional): file name for how to merge fields. Should end with .yml to None.
        endpoint (Endpoint_type_aggregation, optional):endpoint you are aggregating. files or subjects Defaults to "subjects".
    """
    log = "aggregate.log"
    logging.basicConfig(
        filename=log,
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
    )
    logger.info("----------------------")
    logger.info("Starting aggregate run")
    logger.info("----------------------")
    YAMLFILEDIR = "./dags/yaml_merge_and_mapping_dir/merge/"
    if merge_file is None:
        merge_file = f"{YAMLFILEDIR}subject_endpoint_merge.yml"
    else:
        merge_file = f"{YAMLFILEDIR}{merge_file}"

    with open(merge_file) as file:
        how_to_merge = yaml.full_load(file)
    with gzip.open(input_file, "r") as infp:
        with jsonlines.Reader(infp) as readDC:
            entity_rec_mapping = defaultdict(list)
            for rec in readDC:
                id = rec["id"]
                entity_rec_mapping[id].append(rec)
    with gzip.open(output_file, "w") as outfp:
        with jsonlines.Writer(outfp) as writeDC:
            log = LogValidation()
            for id, records in entity_rec_mapping.items():
                if len(records) == 1:
                    merged_entry = records[0]
                else:
                    entities = {k: patient for k, patient in enumerate(records)}
                    lines_ids = list(range(len(records)))
                    if endpoint == "subjects":
                        merged_entry = merge_fields_level(
                            entities, how_to_merge["Patient_merge"], lines_ids
                        )
                        case_ids = [
                            record.get("ResearchSubject")[0].get("id")
                            for record in records
                        ]
                        log = log_merge_error(
                            entities,
                            case_ids,
                            how_to_merge["Patient_merge"],
                            log,
                            endpoint,
                        )
                        merged_entry["ResearchSubject"] = merge_entities_with_same_id(
                            merged_entry["ResearchSubject"],
                            how_to_merge["ResearchSubject_merge"],
                        )
                        for RS in merged_entry["ResearchSubject"]:
                            # RS["File"] = merge_entities_with_same_id(
                            #    RS["File"], how_to_merge["File_merge"]
                            # )
                            RS["Diagnosis"] = merge_entities_with_same_id(
                                RS["Diagnosis"], how_to_merge["Diagnosis_merge"]
                            )
                            RS["Specimen"] = merge_entities_with_same_id(
                                RS["Specimen"], how_to_merge["Specimen_merge"]
                            )
                    else:  # Assume files endpoint
                        merged_entry = merge_fields_level(
                            entities, how_to_merge["File_merge"], lines_ids
                        )
                        file_ids = [record.get("id") for record in records]
                        log = log_merge_error(
                            entities,
                            file_ids,
                            how_to_merge["File_merge"],
                            log,
                            endpoint,
                        )

                writeDC.write(merged_entry)
            log.generate_report(logging.getLogger("test"))


if __name__ == "__main__":
    aggregation()
