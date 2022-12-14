import os
from typing import Dict

from airflow.decorators import task

from cdatransform.transform.mergecli import Merge


def get_dc_files(file_type: str, dc_results: Dict) -> Dict:
    dc_files: Dict = {}

    for k in dc_results:
        if file_type in k:
            dc_files[k.replace(f"_{file_type}", "").lower()] = dc_results[k]
    return dc_files


@task(task_id="subjects_merged")
def subjects_merged(uuid: str, dc_results: Dict):
    dc_files = get_dc_files("subjects", dc_results)
    dest_bucket = os.environ["MERGE_DESTINATION_BUCKET"]
    output_file = f"{dest_bucket}/all_Subjects_{uuid}.jsonl.gz"
    merge = Merge(
        how_to_merge_file="subject_endpoint_merge.yml",
        output_file=output_file,
        idc=dc_files["idc"],
        pdc=dc_files["pdc"],
        gdc=dc_files["gdc"],
    )
    return merge.merge_subjects()


@task(task_id="files_merged")
def files_merged(uuid: str, dc_results: Dict):
    dc_files = get_dc_files("files", dc_results)
    dest_bucket = os.environ["MERGE_DESTINATION_BUCKET"]
    output_file = f"{dest_bucket}/all_Files_{uuid}.jsonl.gz"
    merge = Merge(
        how_to_merge_file="files_endpoint_merge.yml",
        output_file=output_file,
        idc=dc_files["idc"],
        pdc=dc_files["pdc"],
        gdc=dc_files["gdc"],
    )
    return merge.merge_files()
