from airflow.decorators import task
from dags.cdatransform.extract.gdc import GDC
from cdatransform.transform.transform_main import transform_case_or_file
from cdatransform.lib import make_harmonized_file_name
import os
from cdatransform.models.extraction_result import ExtractionResult
from dags.cdatransform.services.storage_service import StorageService
from dags.cdatransform.transform.aggregate import aggregation


@task(task_id="gdc_extract_cases")
def gdc_cases(uuid: str, **kwargs) -> str:
    dest_bucket = os.environ["GDC_DESTINATION_BUCKET"]
    gdc = GDC(dest_bucket)
    file_name = f"gdc.all_cases_{uuid}.jsonl.gz"
    return gdc.save_cases(file_name)


@task(task_id="gdc_extract_files")
def gdc_files(uuid: str, **kwargs) -> str:
    dest_bucket = os.environ["GDC_DESTINATION_BUCKET"]
    gdc = GDC(dest_bucket)
    file_name = f"gdc.all_files_{uuid}.jsonl.gz"
    return gdc.save_files(file_name)


@task(task_id="gdc_transform_cases")
def gdc_transform_cases(uuid: str, extract_result: str, **kwargs):
    output_file = f"gdc.all_cases.{uuid}.H.jsonl.gz"
    storage_service = StorageService()
    return transform_case_or_file(
        storage_service=storage_service,
        bucket_name=os.environ["GDC_DESTINATION_BUCKET"],
        input_path=extract_result,
        output_file=output_file,
        endpoint="cases",
        yaml_mapping_transform_file="GDC_subject_endpoint_mapping.yml")

@task(task_id="gdc_transform")
def gdc_transform_files(uuid: str, extract_result: str, **kwargs):
    output_file = f"gdc.all_files.{uuid}.H.jsonl.gz"
    storage_service = StorageService()
    return transform_case_or_file(
        storage_service=storage_service,
        bucket_name=os.environ["GDC_DESTINATION_BUCKET"],
        input_path=extract_result,
        output_file=output_file,
        endpoint="files",
        yaml_mapping_transform_file="GDC_file_endpoint_mapping.yml")

@task(task_id="gdc_aggregate_cases")
def gdc_aggregate_cases(uuid: str, transform_result: str, **kwargs):
    output_file = f"gdc.all_Subjects.{uuid}.jsonl.gz"
    storage_service = StorageService()
    return aggregation(
        storage_service=storage_service,
        bucket_name=os.environ["GDC_DESTINATION_BUCKET"],
        input_file=transform_result,
        output_file=output_file,
        merge_file="subject_endpoint_merge.yml",
        endpoint="subjects"
    )


@task(task_id="gdc_aggregate_files")
def gdc_aggregate_files(uuid: str, transform_result: str, **kwargs):
    output_file = f"gdc.all_Files.{uuid}.jsonl.gz"
    storage_service = StorageService()
    return aggregation(
        storage_service=storage_service,
        bucket_name=os.environ["GDC_DESTINATION_BUCKET"],
        input_file=transform_result,
        output_file=output_file,
        merge_file="file_endpoint_merge.yml",
        endpoint="files"
    )
