import os
from typing import Dict

from airflow.decorators import task

from cdatransform.load.translate_schema import TransformSchema
from cdatransform.services.context_service import ContextService


@task(task_id="schema_transform")
def schema_transform_task(load_result: Dict):
    transformed_schema_bucket = os.environ["TRANSFORMED_SCHEMA_BUCKET"]
    context_service = ContextService()
    schema_transform = TransformSchema(
        load_result=load_result,
        destination_bucket=transformed_schema_bucket,
        project=context_service.project,
        dataset=context_service.dataset,
    )

    return schema_transform.transform()
