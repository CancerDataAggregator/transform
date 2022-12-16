import argparse
from typing import Optional, Union

from typing_extensions import Literal

from ..lib import yamlPathMapping
from ..transform.yaml_mapping_types import YamlFileMapping
from .schema import Schema

Endpoint_type_schema = Literal["File", "Patient"]


def json_bq_schema(
    out_file: str = "",
    mapping_file: Optional[Union[YamlFileMapping, str]] = None,
    endpoint: Optional[Endpoint_type_schema] = None,
):
    if mapping_file is None:
        raise ValueError("Please enter a mapping file")
    if endpoint is None:
        raise ValueError("Please enter a Endpoint either File or Patient")
    mapping_file = yamlPathMapping(mapping_file)
    schema = Schema(
        mapping=mapping_file,
        outfile=out_file,
        endpoint=endpoint,
    )
    schema.write_json_schema()
