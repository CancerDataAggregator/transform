from dataclasses import dataclass
from enum import Enum
import gzip
import os
from pathlib import Path, PurePath

import sys
import time
from typing import Optional, Type, Union
from typing_extensions import Literal
from .transform_lib.transform_with_YAML_v1 import functionalize_trans_dict, Transform
import jsonlines
import yaml
from yaml import Loader
import logging
from .yaml_mapping_types import YamlFileMapping

logger = logging.getLogger(__name__)


def filter_cases(reader, case_list):
    if case_list is None:
        cases = None
    else:
        cases = set(case_list)

    for case in reader:
        if cases is None:
            yield case
        elif len(cases) == 0:
            break
        elif case.get("id") in cases:
            cases.remove(case.get("id"))
            yield case


from ..lib import get_ids


def print_mapping_files():

    file_list = ""
    for _, _, files in os.walk("./dags/yaml_merge_and_mapping_dir/mapping"):
        for file in files:
            file_list += 'Literal["{}"],'.format(file)

    type_file_list = (
        f"""from typing_extensions import Literal\n from typing import Union\nYamlFileMapping = Union[{file_list}]"""
    )
    open("dags/cdatransform/transform/yaml_mapping_types.py", "w").write(type_file_list)


def print_merge_files():

    file_list = ""
    for _, _, files in os.walk("./dags/yaml_merge_and_mapping_dir/merge"):
        for file in files:
            file_list += 'Literal["{}"],'.format(file)

    type_file_list = (
        f"""from typing_extensions import Literal\n from typing import Union\nYamlFileMerge = Union[{file_list}]"""
    )
    open("dags/cdatransform/transform/yaml_merge_types.py", "w").write(type_file_list)


Endpoint_type = Union[Literal["cases"], Literal["files"]]


def transform_case_or_file(
    input_file: str = "",
    output_file: str = "",
    yaml_mapping_transform_file: Optional[YamlFileMapping] = None,
    endpoint: Endpoint_type = "cases",
    id_lookup_in_jsonl_file_case_or_file: Optional[str] = None,
    ids_lookup_in_jsonl_file_case_or_file: Optional[str] = None,
) -> None:
    YAMLFILEDIR = "./dags/yaml_merge_and_mapping_dir/mapping/"
    if yaml_mapping_transform_file is None:
        yaml_mapping_transform_file = f"{YAMLFILEDIR}GDC_subject_endpoint_mapping.yml"
    else:
        yaml_mapping_transform_file = f"{YAMLFILEDIR}{yaml_mapping_transform_file}"
    # endpoint = ["case","file",]
    # this is a str for the endpoints will change use param later

    mapping_and_transformation: dict[str, dict[str, dict]] = yaml.load(
        open(yaml_mapping_transform_file, "r"), Loader=Loader
    )

    for entity, mapping_or_transformation_dict in mapping_and_transformation.items():
        if "Transformations" in mapping_or_transformation_dict:
            mapping_and_transformation[entity][
                "Transformations"
            ] = functionalize_trans_dict(
                mapping_and_transformation[entity]["Transformations"]
            )
    transform = Transform(mapping_and_transformation, endpoint)
    t0 = time.time()
    count = 0
    id_list = get_ids(
        id_lookup_in_jsonl_file_case_or_file,
        id_list_file=ids_lookup_in_jsonl_file_case_or_file,
    )
    path_str = str(Path(input_file).resolve())
    with gzip.open(path_str, "r") as infp:
        with jsonlines.Reader(infp) as reader:
            with gzip.open(output_file, "w") as outfp:
                with jsonlines.Writer(outfp) as writer:
                    for line in reader:
                        if id_list is None or line.get("id") in id_list:
                            writer.write(transform(line))
                            count += 1
                            if count % 1000 == 0:
                                sys.stderr.write(
                                    f"Processed {count} {endpoint} ({time.time() - t0}).\n"
                                )

    sys.stderr.write(f"Processed {count} {endpoint} ({time.time() - t0}).\n")


if __name__ == "__main__":
    # execute only if run as a script
    transform_case_or_file()
