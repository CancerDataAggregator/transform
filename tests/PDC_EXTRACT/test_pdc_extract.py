from uuid import uuid4

from dags.cdatransform.extract.pdc import PDC


def test_pdc_extract():
    uuid = str(uuid4().hex)
    pdc = PDC(dest_bucket="gs://broad-cda-dev/airflow_testing", uuid=uuid)

    # pdc.save_cases("pdc.all_cases.jsonl.gz")
    result = pdc.save_cases(f"pdc.all_cases_{uuid}.jsonl.gz")
    print(result)
