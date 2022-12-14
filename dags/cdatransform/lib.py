import os
from typing import Optional, Union

"""Program wide utility functions and classes."""


def get_ids(id: Optional[str] = None, id_list_file: Optional[str] = None):
    if id is not None:
        return [id]

    if id_list_file is None:
        return None
    else:
        with open(id_list_file, "r") as fp:
            return [line.strip() for line in fp.readlines()]


def yamlPathMapping(yaml_mapping_file=None, merge_or_mapping="mapping"):
    if yaml_mapping_file is None:
        raise ValueError("Please enter a path")

    if "MERGE_AND_MAPPING_DIRECTORY" in os.environ:
        mapping_dir = os.environ["MERGE_AND_MAPPING_DIRECTORY"]
    else:
        mapping_dir = "./dags/yaml_merge_and_mapping_dir"
    YAMLFILEDIR = f"{mapping_dir}/{merge_or_mapping}/"
    return f"{YAMLFILEDIR}{yaml_mapping_file}"


def make_harmonized_file_name(file_name: str):
    return file_name.replace(".jsonl", ".H.jsonl")
