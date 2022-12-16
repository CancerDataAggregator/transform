import os
from typing import Dict

from airflow.decorators import task

from dags.cdatransform.load.translate_schema import TransformSchema


@task(task_id="schema_transform")
def schema_transform_task(load_result: Dict):
    transformed_schema_bucket = os.environ["TRANSFORMED_SCHEMA_BUCKET"]
    TransformSchema(
        load_result=load_result, destination_bucket=transformed_schema_bucket
    )
