from airflow.decorators import task
from cdatransform.extract.gdc import GDC
from cdatransform.lib import get_case_ids
from datetime import datetime


@task(task_id="gdc_extract")
def gdc_extract(**kwargs):
    print("Extracting GDC")
    gdc = GDC(make_spec_file=f"gdc-extract-{str(datetime.now())}.jsonl.gz")
    gdc.save_cases(
        f"gdc-save-{str(datetime.now())}.jsonl.gz",
        case_ids=get_case_ids(case=None, case_list_file=None),
    )
    return "gdc_file.jsonl.gz"


@task(task_id="gdc_transform")
def gdc_transform(transform_result: str, **kwargs):
    print("Transforming GDC")

    return "gdc_transformed.jsonl.gz"


@task(task_id="gdc_load")
def gdc_load(transform_result: str, **kwargs):
    print("Loading GDC")

    return "gdc_loaded.jsonl.gz"
