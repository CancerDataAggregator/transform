from typing import Union, Literal

YamlFileMapping = Union[
    Literal["IDC_mapping.yml"],
    Literal["GDC_subject_endpoint_mapping.yml"],
    Literal["PDC_subject_endpoint_mapping.yml"],
    Literal["GDC_mapping.yml"],
    Literal["GDC_file_endpoint_mapping.yml"],
    Literal["PDC_file_endpoint_mapping.yml"],
    Literal["PDC_mapping.yml"],
]
