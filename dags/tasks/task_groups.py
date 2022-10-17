from airflow.decorators import task_group
from dags.tasks.pdc import pdc_aggregate_cases, pdc_aggregate_files, pdc_cases, pdc_files, pdc_transform_cases, pdc_transform_files
from tasks.gdc import gdc_aggregate_cases, gdc_aggregate_files, gdc_transform_cases, gdc_transform_files
from tasks.gdc import gdc_cases, gdc_files
from tasks.idc import idc_extract, idc_load, idc_transform

#region GDC
@task_group(group_id="GDC_Cases")
def gdc_cases_task_group(uuid: str, **kwargs):
    return gdc_aggregate_cases(uuid, gdc_transform_cases(gdc_cases(uuid)))

@task_group(group_id="GDC_Files")
def gdc_files_task_group(uuid: str, **kwargs):
    return gdc_aggregate_files(uuid, gdc_transform_files(gdc_files(uuid)))

@task_group(group_id="GDC")
def gdc_task_group(uuid: str, **kwargs):
    return {
        "gdc_cases": gdc_cases_task_group(uuid),
        "gdc_files": gdc_files_task_group(uuid)
    }
#endregion

#region IDC
@task_group(group_id="IDC")
def idc_task_group(**kwargs):
    return {"idc": idc_load(idc_transform(idc_extract()))}
#endregion

#region PDC
@task_group(group_id="PDC_Cases")
def pdc_cases_task_group(uuid: str, **kwargs):
    return pdc_aggregate_cases(uuid, pdc_transform_cases(pdc_cases(uuid)))

@task_group(group_id="PDC_Files")
def pdc_files_task_group(uuid: str, **kwargs):
    return pdc_aggregate_files(uuid, pdc_transform_files(pdc_files(uuid)))

@task_group(group_id="PDC")
def pdc_task_group(uuid: str, **kwargs):
    return {
        "pdc_cases": pdc_cases_task_group(uuid),
        "pdc_files": pdc_files_task_group(uuid)
    }
#endregion

@task_group(group_id="dc_group")
def dc_task_group(uuid:str):
    return {**gdc_task_group(uuid), **idc_task_group(), **pdc_task_group(uuid)}
