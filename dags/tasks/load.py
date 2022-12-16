import os
from typing import Dict

from airflow.decorators import task


@task(task_id="load_subjects")
def load_subjects(version: str):
    table_id = f"all_Subjects_{version}_final"
    print(table_id)
    return {"Subjects": table_id}


@task(task_id="load_files")
def load_files(version: str):
    table_id = f"all_Files_{version}_final"
    print(table_id)
    return {"Files": table_id}
