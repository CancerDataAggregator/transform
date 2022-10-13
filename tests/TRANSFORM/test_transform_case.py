from dags.cdatransform.transform.transform_main import (
    transform_case_or_file,
    print_mapping_files,
    print_merge_files,
)

transform_case_or_file(
    input_file="tests_out.jsonl.gz",
    output_file="test_out_transformed.gz",
    endpoint="cases",
    yaml_mapping_transform_file="GDC_subject_endpoint_mapping.yml",
)


# transform_case_or_file(
#     input_file="tests_out.jsonl.gz",
#     output_file="test_out_transformed.gz",
#     endpoint="files",
#     yaml_mapping_transform_file="GDC_file_endpoint_mapping.yml",
# )
