import os
from typing import Dict

from airflow.decorators import task_group
from tasks.gdc import (
    gdc_aggregate_cases,
    gdc_aggregate_files,
    gdc_cases,
    gdc_files,
    gdc_transform_cases,
    gdc_transform_files,
)
from tasks.idc import (
    idc_cases_to_bucket,
    idc_cases_to_table,
    idc_combine_case_blobs,
    idc_combine_file_blobs,
    idc_files_to_bucket,
    idc_files_to_table,
)
from tasks.merge import files_merged, subjects_merged
from tasks.pdc import (
    pdc_aggregate_cases,
    pdc_aggregate_files,
    pdc_cases,
    pdc_files,
    pdc_transform_cases,
    pdc_transform_files,
)


# region GDC
@task_group(group_id="GDC_Cases")
def gdc_cases_task_group(uuid: str, **kwargs):
    return gdc_aggregate_cases(uuid, gdc_transform_cases(uuid, gdc_cases(uuid)))


@task_group(group_id="GDC_Files")
def gdc_files_task_group(uuid: str, **kwargs):
    return gdc_aggregate_files(uuid, gdc_transform_files(uuid, gdc_files(uuid)))


@task_group(group_id="GDC")
def gdc_task_group(uuid: str, **kwargs):
    return {
        "gdc_cases": gdc_cases_task_group(uuid),
        "gdc_files": gdc_files_task_group(uuid),
    }


# endregion

# region IDC
@task_group(group_id="IDC_Cases_Extract")
def idc_cases_extract_task_group(uuid: str, **kwargs):
    version = os.environ["IDC_DATA_VERSION"]
    return idc_combine_case_blobs(
        idc_cases_to_bucket(idc_cases_to_table(uuid, version))
    )


@task_group(group_id="IDC_Files_Extract")
def idc_files_extract_task_group(uuid: str, **kwargs):
    version = os.environ["IDC_DATA_VERSION"]
    return idc_combine_file_blobs(
        idc_files_to_bucket(idc_files_to_table(uuid, version))
    )


@task_group(group_id="IDC")
def idc_task_group(uuid: str, **kwargs):
    return {
        "idc_cases": idc_cases_extract_task_group(uuid),
        "idc_files": idc_files_extract_task_group(uuid),
    }


# endregion

# region PDC
@task_group(group_id="PDC_Cases")
def pdc_cases_task_group(uuid: str, **kwargs):
    return pdc_aggregate_cases(uuid, pdc_transform_cases(uuid, pdc_cases(uuid)))


@task_group(group_id="PDC_Files")
def pdc_files_task_group(uuid: str, **kwargs):
    return pdc_aggregate_files(uuid, pdc_transform_files(uuid, pdc_files(uuid)))


@task_group(group_id="PDC")
def pdc_task_group(uuid: str, **kwargs):
    return {
        "pdc_cases": pdc_cases_task_group(uuid),
        "pdc_files": pdc_files_task_group(uuid),
    }


# endregion


@task_group(group_id="dc_group")
def dc_task_group(uuid: str):
    return {**gdc_task_group(uuid), **idc_task_group(uuid), **pdc_task_group(uuid)}


@task_group(group_id="merge_group")
def merge_task_group(dc_results: Dict):
    return {
        "cases_merged": subjects_merged(dc_results),
        "files_merged": files_merged(dc_results),
    }


@task_group(group_id="load_group")
def load_task_group(merge_result: Dict):
    return "Success"
