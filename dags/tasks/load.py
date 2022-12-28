import os
from typing import Dict

from airflow.decorators import task

from dags.cdatransform.load.Load import Load
from dags.cdatransform.services.context_service import ContextService


@task(task_id="load_subjects")
def load_subjects(version: str, load_data: Dict):
    context_service = ContextService()
    dest_table = f"all_Subjects_{version}_final"
    table_id = f"{context_service.project}.{context_service.dataset}.{dest_table}"
    loader = Load(
        context_service.project,
        context_service.dataset,
        data_file=load_data["subjects_merged"],
        dest_table_id=table_id,
        schema=load_data["subjects_schema"],
    )
    loader.load_data()

    return {"Subjects": dest_table}


@task(task_id="load_files")
def load_files(version: str, load_data: Dict):
    dest_table = f"all_Files_{version}_final"

    context_service = ContextService()
    table_id = f"{context_service.project}.{context_service.dataset}.{dest_table}"
    loader = Load(
        context_service.project,
        context_service.dataset,
        data_file=load_data["files_merged"],
        dest_table_id=table_id,
        schema=load_data["files_schema"],
    )
    loader.load_data()
    return {"Files": table_id}
