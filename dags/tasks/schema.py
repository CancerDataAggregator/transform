import os
from typing import Dict

from airflow.decorators import task

from cdatransform.load.JSON_schema import json_bq_schema


@task(task_id="subjects_schema_generation")
def subjects_schema(uuid: str):
    dest_bucket = os.environ["GENERATED_SCHEMA_BUCKET"]
    output_file = f"{dest_bucket}/all_Subjects_Schema_{uuid}.json"
    return json_bq_schema(
        output_file=output_file,
        mapping_file="GDC_subject_endpoint_mapping.yml",
        endpoint="Patient",
    )


@task(task_id="files_schema_generation")
def files_schema(uuid: str):
    dest_bucket = os.environ["GENERATED_SCHEMA_BUCKET"]
    output_file = f"{dest_bucket}/all_Files_Schema_{uuid}.json"
    return json_bq_schema(
        output_file=output_file,
        mapping_file="GDC_file_endpoint_mapping.yml",
        endpoint="Files",
    )
