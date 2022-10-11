from typing import Union

"""Program wide utility functions and classes."""


def get_ids(id: str = None, id_list_file: str = None) -> Union[list[str], None]:
    if id is not None:
        return [id]

    if id_list_file is None:
        return None
    else:
        with open(id_list_file, "r") as fp:
            return [line.strip() for line in fp.readlines()]


def yamlPathMapping(yaml_mapping_file=None):
    if yaml_mapping_file is None:
        raise ValueError("Please enter a path")

    YAMLFILEDIR = "./dags/yaml_merge_and_mapping_dir/mapping/"
    return f"{YAMLFILEDIR}{yaml_mapping_file}"
