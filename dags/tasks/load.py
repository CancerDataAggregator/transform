import os
from typing import Dict

from airflow.decorators import task

from cdatransform.load.Load import Load

# from cdatransform.services.context_service import ContextService


@task(task_id="load_subjects")
def load_subjects(version: str, load_data: Dict):
    # context_service = ContextService()
    dest_table = f"all_Subjects_{version}_final"
    table_id = f"gdc-bq-sample.dev.{dest_table}"
    loader = Load(
        "gdc-bq-sample",
        "dev",
        data_file=load_data["subjects_merged"],
        dest_table_id=table_id,
        schema=load_data["subjects_schema"],
    )
    loader.load_data()

    return dest_table


@task(task_id="load_files")
def load_files(version: str, load_data: Dict):
    dest_table = f"all_Files_{version}_final"

    # context_service = ContextService()
    table_id = f"gdc-bq-sample.dev.{dest_table}"
    loader = Load(
        "gdc-bq-sample",
        "dev",
        data_file=load_data["files_merged"],
        dest_table_id=table_id,
        schema=load_data["files_schema"],
    )
    loader.load_data()
    return table_id
