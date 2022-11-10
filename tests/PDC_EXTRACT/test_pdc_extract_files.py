import os
from uuid import uuid4
from dags.cdatransform.extract.pdc import PDC
from google.cloud import storage
from google.cloud.storage import Client

from dags.cdatransform.services.storage_service import StorageService

uuid = str(uuid4().hex)
pdc = PDC(dest_bucket="gs://broad-cda-dev/airflow_testing", uuid=uuid)

# pdc.save_cases("pdc.all_cases.jsonl.gz")
result = pdc.save_files(f"pdc.all_files_{uuid}.jsonl.gz")
print(result)
