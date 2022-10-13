from airflow.decorators import task_group
from tasks.gdc import gdc_aggregate_cases, gdc_aggregate_files, gdc_transform_cases, gdc_transform_files
from tasks.gdc import gdc_cases, gdc_files
from tasks.idc import idc_extract, idc_load, idc_transform
from tasks.pdc import pdc_extract, pdc_load, pdc_transform

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

@task_group(group_id="IDC")
def idc_task_group(**kwargs):
    return {"idc": idc_load(idc_transform(idc_extract()))}


@task_group(group_id="PDC")
def pdc_task_group(uuid,**kwargs):
    return {"pdc": pdc_load(pdc_transform(pdc_extract(uuid)))}


@task_group(group_id="dc_group")
def dc_task_group(uuid:str):
    return {**gdc_task_group(uuid), **idc_task_group(), **pdc_task_group(uuid)}
