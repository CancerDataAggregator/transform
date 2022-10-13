from airflow.decorators import task
from cdatransform.extract.pdc import PDC
from datetime import datetime


@task(task_id="pdc_extract")
def pdc_extract(uuid:str,**kwargs):
    print("Extracting PDC")
    pdc = PDC()
    file_name = f"pdc-save-{uuid}.jsonl.gz"
    pdc.save_cases(file_name)
    return file_name


@task(task_id="pdc_transform")
def pdc_transform(extraction_result: str, **kwargs):
    print("Transforming PDC")

    return "pdc_transformed.jsonl.gz"


@task(task_id="pdc_load")
def pdc_load(transform_result: str, **kwargs):
    print("Loading PDC")

    return "pdc_loaded.jsonl.gz"
