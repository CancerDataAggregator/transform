from airflow.decorators import task
from dags.cdatransform.extract.gdc import GDC
from cdatransform.transform.transform_main import transform_case_or_file
from cdatransform.lib import make_harmonized_file_name
import os
from cdatransform.models.extraction_result import ExtractionResult
from dags.cdatransform.services.storage_service import StorageService


@task(task_id="gdc_extract_cases")
def gdc_cases(uuid: str, **kwargs) -> ExtractionResult:
    dest_bucket = os.environ["GDC_DESTINATION_BUCKET"]
    gdc = GDC(dest_bucket)
    file_name = f"gdc.all_cases_{uuid}"
    return gdc.save_cases(file_name)


@task(task_id="gdc_extract_files")
def gdc_files(uuid: str, **kwargs) -> ExtractionResult:
    dest_bucket = os.environ["GDC_DESTINATION_BUCKET"]
    gdc = GDC(dest_bucket)
    file_name = f"gdc.all_files_{uuid}"
    return gdc.save_files(file_name)

def transform_file_loop(extract_result: ExtractionResult, endpoint: str, mapping_file: str):
    for file_path in extract_result.file_loop():
        output_file_name = make_harmonized_file_name(file_path)
        storage_service = StorageService()
        transform_case_or_file(
            storage_service=storage_service,
            bucket_name=extract_result.bucket_name,
            input_file=file_path,
            output_file=output_file_name,
            endpoint=endpoint,
            yaml_mapping_transform_file=mapping_file,
        )


@task(task_id="gdc_transform_cases")
def gdc_transform_cases(extract_result: ExtractionResult, **kwargs):
    transform_file_loop(extract_result=extract_result, endpoint="cases", mapping_file="GDC_subject_endpoint_mapping.yml")

    return ""


@task(task_id="gdc_transform")
def gdc_transform_files(extract_result: ExtractionResult, **kwargs):
    transform_file_loop(extract_result=extract_result, endpoint="files", mapping_file="GDC_file_endpoint_mapping.yml")

    return ""


@task(task_id="gdc_aggregate_cases")
def gdc_aggregate_cases(uuid: str, transform_result: str, **kwargs):
    return f"gdc.all_Subjects.{uuid}.jsonl.gz"


@task(task_id="gdc_aggregate_files")
def gdc_aggregate_files(uuid: str, transform_result: str, **kwargs):
    return f"gdc.all_Files.{uuid}.jsonl.gz"
