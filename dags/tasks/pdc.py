from airflow.decorators import task
from ..cdatransform.extract.pdc import PDC
from cdatransform.lib import get_case_ids
from datetime import datetime


@task(task_id="pdc_extract")
def pdc_extract(**kwargs):
    print("Extracting PDC")
    pdc = PDC()
    file_name = f"pdc-save-{str(datetime.now())}.jsonl.gz"
    pdc.save_cases(file_name, case_ids=get_case_ids(case=None, case_list_file=None))
    return file_name


@task(task_id="pdc_transform")
def pdc_transform(extraction_result: str, **kwargs):
    print("Transforming PDC")

    return "pdc_transformed.jsonl.gz"


@task(task_id="pdc_load")
def pdc_load(transform_result: str, **kwargs):
    print("Loading PDC")

    return "pdc_loaded.jsonl.gz"
