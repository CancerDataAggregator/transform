import os
from typing import Dict
from airflow.decorators import task
from cdatransform.extract.idc import IDC
from cdatransform.transform.transform_main import Endpoint_type


def instantiateIDC(uuid: str, version: str, endpoint: Endpoint_type) -> IDC:
    out_file = f"idc.all_{endpoint}_{version}_{uuid}.jsonl.gz"
    project_dataset = os.environ["DESTINATION_PROJECT_AND_DATASET"]
    dest_table_id = f"{project_dataset}.IDC_{endpoint}_V{version}_{uuid}"
    source_table = f"bigquery-public-data.idc_v{version}.dicom_pivot_v{version}"
    dest_bucket = os.environ["IDC_DESTINATION_BUCKET"]
    dest_bucket_file_name = f"idc_v{version}_{endpoint}_{uuid}_*.jsonl.gz"

    return IDC(
        bq_project_dataset=project_dataset,
        dest_table_id=dest_table_id,
        source_table=source_table,
        endpoint=endpoint,
        dest_bucket=dest_bucket,
        dest_bucket_file_name=dest_bucket_file_name,
        out_file=out_file,
    )


@task(task_id="idc_extract_cases_to_table")
def idc_cases_to_table(uuid: str, version: str, **kwargs):
    idc = instantiateIDC(uuid, version, "Patient")

    idc.query_idc_to_table()

    return {'version': version, 'uuid': uuid}


@task(task_id="idc_extract_cases_to_blobs")
def idc_cases_to_bucket(context: Dict, **kwargs):
    print(context)
    idc = instantiateIDC(context['uuid'], context['version'], "Patient")

    idc.table_to_bucket()

    return context


@task(task_id="idc_extract_files_to_table")
def idc_files_to_table(uuid: str, version: str, **kwargs):
    idc = instantiateIDC(uuid, version, "File")

    idc.query_idc_to_table()

    return {'version': version, 'uuid': uuid}


@task(task_id="idc_extract_files_to_blobs")
def idc_files_to_bucket(context: Dict, **kwargs):
    print(context)
    idc = instantiateIDC(context['uuid'], context['version'], "File")

    idc.table_to_bucket()

    return context


@task(task_id="idc_combine_case_blobs")
def idc_combine_case_blobs(context: Dict, **kwargs):
    idc = instantiateIDC(context["uuid"], context["version"], "Patient")

    return idc.download_blob()


@task(task_id="idc_combine_file_blobs")
def idc_combine_file_blobs(context: Dict, **kwargs):
    idc = instantiateIDC(context["uuid"], context["version"], "File")

    return idc.download_blob()
