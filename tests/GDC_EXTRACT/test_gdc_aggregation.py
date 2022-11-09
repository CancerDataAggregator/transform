from dags.cdatransform.services.storage_service import StorageService
from dags.cdatransform.transform.aggregate import aggregation

output_file = f"gdc.all_Subjects.846dae18acf0467dbfb6d02bfd0e95a4.jsonl.gz"
storage_service = StorageService()

aggregation(
    storage_service=storage_service,
    bucket_name="gs://broad-cda-dev/airflow_testing",
    input_file="gs://broad-cda-dev/airflow_testing/gdc.all_cases.846dae18acf0467dbfb6d02bfd0e95a4.H.jsonl.gz",
    output_file=output_file,
    merge_file="subject_endpoint_merge.yml",
    endpoint="subjects"
)
