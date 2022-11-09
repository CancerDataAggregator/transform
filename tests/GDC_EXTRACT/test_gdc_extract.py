from uuid import uuid4
from dags.cdatransform.extract.gdc import GDC

# GDC().save_cases("gdc.all_cases.jsonl.gz")
uuid = str(uuid4().hex)
GDC(dest_bucket="gs://broad-cda-dev/airflow_testing").save_cases(f"gdc.all_cases_{uuid}.jsonl.gz")
