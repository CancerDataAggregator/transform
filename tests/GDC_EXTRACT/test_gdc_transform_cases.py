from dags.cdatransform.models.extraction_result import ExtractionResult
from dags.cdatransform.services.storage_service import StorageService
from dags.cdatransform.lib import make_harmonized_file_name
from dags.cdatransform.transform.transform_main import transform_case_or_file

# broad-cda-dev/airflow_testing/gdc.all_cases_60aa41cf7d9d4532b332d51dc5527e4e-index-0.jsonl.gz
extraction_result = ExtractionResult("gdc.all_cases_60aa41cf7d9d4532b332d51dc5527e4e", 3, "gs://broad-cda-dev/airflow_testing")
for file_path in extraction_result.file_loop():
        output_file_name = make_harmonized_file_name(file_path)
        storage_service = StorageService()
        transform_case_or_file(
            storage_service=storage_service,
            bucket_name=extraction_result.bucket_name,
            input_file=file_path,
            output_file=output_file_name,
            endpoint="cases",
            yaml_mapping_transform_file="GDC_subject_endpoint_mapping.yml",
        )
