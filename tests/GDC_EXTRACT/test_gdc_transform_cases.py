from dags.cdatransform.models.extraction_result import ExtractionResult
from dags.cdatransform.services.storage_service import StorageService
from dags.cdatransform.lib import make_harmonized_file_name
from dags.cdatransform.transform.transform_main import transform_case_or_file

# broad-cda-dev/airflow_testing/gdc.all_cases_53947865cecd4fb9abf073af1f54b467-index-3.jsonl.gz

output_file = f"gdc.all_cases.846dae18acf0467dbfb6d02bfd0e95a4.H.jsonl.gz"
storage_service = StorageService()
transform_case_or_file(
    storage_service=storage_service,
    bucket_name="gs://broad-cda-dev/airflow_testing",
    input_path="gs://broad-cda-dev/airflow_testing/gdc.all_cases_846dae18acf0467dbfb6d02bfd0e95a4.jsonl.gz",
    output_file=output_file,
    endpoint="cases",
    yaml_mapping_transform_file="GDC_subject_endpoint_mapping.yml")
