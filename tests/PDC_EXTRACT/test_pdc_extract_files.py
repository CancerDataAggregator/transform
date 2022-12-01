import os
from uuid import uuid4

from google.cloud import storage
from google.cloud.storage import Client

from dags.cdatransform.extract.pdc import PDC
from dags.cdatransform.services.storage_service import StorageService
from dags.cdatransform.transform.aggregate import aggregation
from dags.cdatransform.transform.transform_main import transform_case_or_file

# uuid = str(uuid4().hex)
# pdc = PDC(dest_bucket="gs://broad-cda-dev/airflow_testing", uuid=uuid)

# pdc.save_cases("pdc.all_cases.jsonl.gz")
# result = pdc.save_files(f"pdc.all_files_{uuid}.jsonl.gz")
# extract_result = "gs://broad-cda-dev/airflow_testing/pdc.all_files_90838a4fe85b4870ae6b5d1e59069aae.jsonl.gz"
# output_file = f"pdc.all_files.90838a4fe85b4870ae6b5d1e59069aae.H.jsonl.gz"
# storage_service = StorageService()
# result = transform_case_or_file(
#     storage_service=storage_service,
#     bucket_name="gs://broad-cda-dev/airflow_testing",
#     input_path=extract_result,
#     output_file=output_file,
#     endpoint="files",
#     yaml_mapping_transform_file="PDC_file_endpoint_mapping.yml",
# )

transform_result = "gs://broad-cda-dev/airflow_testing/pdc.all_files.90838a4fe85b4870ae6b5d1e59069aae.H.jsonl.gz"
output_file = f"pdc.all_Files.90838a4fe85b4870ae6b5d1e59069aae.jsonl.gz"
storage_service = StorageService()
result = aggregation(
    storage_service=storage_service,
    bucket_name="gs://broad-cda-dev/airflow_testing",
    input_file=transform_result,
    output_file=output_file,
    merge_file="file_endpoint_merge.yml",
    endpoint="files",
)
print(result)
