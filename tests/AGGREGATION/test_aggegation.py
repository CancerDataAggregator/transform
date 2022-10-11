from dags.cdatransform.transform.aggregate import aggregation

aggregation(
    input_file="test_out_transformed.gz",
    output_file="gdc.all_Subjects.jsonl.gz ",
    merge_file="subject_endpoint_merge.yml",
    endpoint="subjects",
)
