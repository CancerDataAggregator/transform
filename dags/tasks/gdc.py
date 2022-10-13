from airflow.decorators import task
from cdatransform.extract.gdc import GDC
from cdatransform.transform.transform_main import transform_case_or_file


@task(task_id="gdc_extract")
def gdc_extract(**kwargs):
    print("Extracting GDC")
    gdc = GDC()
    gdc.save_cases("gdc.all_cases.jsonl.gz")
    gdc.save_files("gdc.all_files.jsonl.gz")
    return "gdc_file.jsonl.gz"


@task(task_id="gdc_transform")
def gdc_transform(transform_result: str, **kwargs):
    print("Transforming GDC")
    transform_case_or_file(
        input_file="gdc.all_cases.jsonl.gz",
        output_file="gdc.all_cases.H.jsonl.gz",
        endpoint="cases",
        yaml_mapping_transform_file="GDC_subject_endpoint_mapping.yml",
    )

    transform_case_or_file(
        input_file="gdc.all_files.jsonl.gz",
        output_file="gdc.all_files.H.jsonl.gz",
        endpoint="files",
        yaml_mapping_transform_file="GDC_file_endpoint_mapping.yml",
    )

    return "gdc.all_files.H.jsonl.gz"


@task(task_id="gdc_load")
def gdc_load(transform_result: str, **kwargs):
    print("Loading GDC")

    return "gdc_loaded.jsonl.gz"
