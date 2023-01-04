from dags.cdatransform.load.JSON_schema import json_bq_schema


def test_json_schema():
    json_bq_schema(
        out_file="subjects_schema.json",
        mapping_file="GDC_subject_endpoint_mapping.yml",
        endpoint="Patient",
    )
