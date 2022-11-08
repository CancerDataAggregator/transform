from dags.cdatransform.services.storage_service import StorageService
from dags.cdatransform.transform.aggregate import aggregation

output_file = f"gdc.all_Subjects.53947865cecd4fb9abf073af1f54b467.jsonl.gz"
storage_service = StorageService()

aggregation(
    storage_service=storage_service,
    bucket_name="gs://broad-cda-dev/airflow_testing",
    input_file="gs://broad-cda-dev/airflow_testing/gdc.all_cases.53947865cecd4fb9abf073af1f54b467.H.jsonl.gz",
    output_file=output_file,
    merge_file="subject_endpoint_merge.yml",
    endpoint="subjects"
)
