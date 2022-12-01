from typing import Dict

from airflow.decorators import task


@task(task_id="subjects_merged")
def subjects_merged(dc_results: Dict):
    print(dc_results)
    return "merged_subjects_file.jsonl.gz"


@task(task_id="files_merged")
def files_merged(dc_results: Dict):
    print(dc_results)
    return "merged_files_file.jsonl.gz"
