from airflow.decorators import task
from cdatransform.extract.pdc import PDC
from datetime import datetime

from dags.cdatransform.lib import make_harmonized_file_name
from dags.cdatransform.transform.transform_main import transform_case_or_file


@task(task_id="pdc_cases")
def pdc_cases(uuid:str,**kwargs):
    pdc = PDC()
    file_name = f"pdc-save-cases-{uuid}.jsonl.gz"
    pdc.save_cases(file_name)
    return file_name

@task(task_id="pdc_files")
def pdc_files(uuid:str,**kwargs):
    pdc = PDC()
    file_name = f"pdc-save-files-{uuid}.jsonl.gz"
    pdc.save_files(file_name)
    return file_name


@task(task_id="pdc_transform_cases")
def pdc_transform_cases(extract_result: str, **kwargs):
    output_file_name = make_harmonized_file_name(extract_result)
    transform_case_or_file(
        input_file=extract_result,
        output_file=output_file_name,
        endpoint="cases",
        yaml_mapping_transform_file="PDC_subject_endpoint_mapping.yml",
    )

    return output_file_name

@task(task_id="pdc_transform")
def pdc_transform_files(extract_result: str, **kwargs):
    output_file_name = make_harmonized_file_name(extract_result)
    transform_case_or_file(
        input_file=extract_result,
        output_file=output_file_name,
        endpoint="files",
        yaml_mapping_transform_file="PDC_file_endpoint_mapping.yml",
    )

    return output_file_name

@task(task_id="pdc_aggregate_cases")
def pdc_aggregate_cases(uuid:str, transform_result: str, **kwargs):
    return f"gdc.all_Subjects.{uuid}.jsonl.gz"

@task(task_id="pdc_aggregate_files")
def pdc_aggregate_files(uuid:str, transform_result: str, **kwargs):
    return f"gdc.all_Files.{uuid}.jsonl.gz"
