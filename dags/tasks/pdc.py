import os
from airflow.decorators import task
from cdatransform.extract.pdc import PDC
from datetime import datetime

from cdatransform.lib import make_harmonized_file_name
from cdatransform.transform.transform_main import transform_case_or_file
from dags.cdatransform.services.storage_service import StorageService
from dags.cdatransform.transform.aggregate import aggregation


@task(task_id="pdc_cases")
def pdc_cases(uuid: str, **kwargs):
    pdc = PDC(dest_bucket=os.environ["PDC_DESTINATION_BUCKET"], uuid=uuid)
    file_name = f"pdc.all_cases_{uuid}.jsonl.gz"
    return pdc.save_cases(file_name)


@task(task_id="pdc_files")
def pdc_files(uuid: str, **kwargs):
    pdc = PDC(dest_bucket=os.environ["PDC_DESTINATION_BUCKET"], uuid=uuid)
    file_name = f"pdc.all_files_{uuid}.jsonl.gz"
    return pdc.save_files(file_name)


@task(task_id="pdc_transform_cases")
def pdc_transform_cases(uuid: str, extract_result: str, **kwargs):
    output_file = f"pdc.all_cases.{uuid}.H.jsonl.gz"
    storage_service = StorageService()
    return transform_case_or_file(
        storage_service=storage_service,
        bucket_name=os.environ["PDC_DESTINATION_BUCKET"],
        input_path=extract_result,
        output_file=output_file,
        endpoint="cases",
        yaml_mapping_transform_file="PDC_subject_endpoint_mapping.yml")


@task(task_id="pdc_transform")
def pdc_transform_files(uuid: str, extract_result: str, **kwargs):
    output_file = f"pdc.all_files.{uuid}.H.jsonl.gz"
    storage_service = StorageService()
    return transform_case_or_file(
        storage_service=storage_service,
        bucket_name=os.environ["PDC_DESTINATION_BUCKET"],
        input_path=extract_result,
        output_file=output_file,
        endpoint="files",
        yaml_mapping_transform_file="PDC_file_endpoint_mapping.yml")


@task(task_id="pdc_aggregate_cases")
def pdc_aggregate_cases(uuid: str, transform_result: str, **kwargs):
    output_file = f"pdc.all_Subjects.{uuid}.jsonl.gz"
    storage_service = StorageService()
    return aggregation(
        storage_service=storage_service,
        bucket_name=os.environ["PDC_DESTINATION_BUCKET"],
        input_file=transform_result,
        output_file=output_file,
        merge_file="subject_endpoint_merge.yml",
        endpoint="subjects"
    )


@task(task_id="pdc_aggregate_files")
def pdc_aggregate_files(uuid: str, transform_result: str, **kwargs):
    output_file = f"pdc.all_Files.{uuid}.jsonl.gz"
    storage_service = StorageService()
    return aggregation(
        storage_service=storage_service,
        bucket_name=os.environ["PDC_DESTINATION_BUCKET"],
        input_file=transform_result,
        output_file=output_file,
        merge_file="file_endpoint_merge.yml",
        endpoint="files"
    )
