from typing import Dict
from airflow.decorators import task
from cdatransform.extract.idc import IDC
from cdatransform.transform.transform_main import Endpoint_type
from yaml import Loader, load

def instantiateIDC(uuid: str, version: str, endpoint: Endpoint_type) -> IDC:
    out_file = f"idc.all_{endpoint}_{version}_{uuid}.jsonl.gz"
    dest_table_id = f"gdc-bq-sample.dev.IDC_{endpoint}_V{version}"
    source_table = f"biquery-public-data.idc_v{version}.dicom_pivot_v{version}"
    dest_bucket = "gdc-bq-sample-bucket"
    dest_bucket_file_name = f"idc_v{version}_{endpoint}*.jsonl.gz"
    gsa_key = "../../GCS-service-account-key.json"
    mapping_file = "IDC_mapping.yml"

    mapping = load(open(mapping_file, "r"), Loader=Loader)

    return IDC(gsa_key=gsa_key,
              dest_table_id=dest_table_id,
              mapping=mapping,
              source_table=source_table,
              endpoint="Patient",
              dest_bucket=dest_bucket,
              dest_bucket_file_name=dest_bucket_file_name,
              out_file=out_file)


@task(task_id="idc_extract_cases_to_table")
def idc_cases_to_table(uuid: str, version: str, **kwargs):
    idc = instantiateIDC(uuid, version, "Patient")

    idc.query_idc_to_table()

    return { version: version, uuid: uuid }

@task(task_id="idc_extract_cases_to_blobs")
def idc_cases_to_bucket(context: Dict, **kwargs):
    idc = instantiateIDC(context['uuid'], context['version'], "Patient")

    idc.table_to_bucket()

    return context

@task(task_id="idc_extract_files_to_table")
def idc_files_to_table(uuid: str, version: str, **kwargs):
    idc = instantiateIDC(uuid, version, "File")

    idc.query_idc_to_table()

    return { version: version, uuid: uuid }

@task(task_id="idc_extract_files_to_blobs")
def idc_files_to_bucket(context: Dict, **kwargs):
    idc = instantiateIDC(context['uuid'], context['version'], "File")

    idc.table_to_bucket()

    return context

@task(task_id="idc_combine_case_blobs")
def idc_combine_case_blobs(context: Dict, **kwargs):
    idc = instantiateIDC(context['uuid'], context['version'], "Patient")

    idc.download_blob()

    return idc.out_file

@task(task_id="idc_combine_file_blobs")
def idc_combine_file_blobs(context: Dict, **kwargs):
    idc = instantiateIDC(context['uuid'], context['version'], "File")

    idc.download_blob()

    return idc.out_file

@task(task_id="idc_transform")
def idc_transform(transform_result: str, **kwargs):
    print("Transforming IDC")

    return "idc_transformed.jsonl.gz"


@task(task_id="idc_load")
def idc_load(transform_result: str, **kwargs):
    print("Loading IDC")

    return "idc_loaded.jsonl.gz"
