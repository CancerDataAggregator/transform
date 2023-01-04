from uuid import uuid4

from dags.cdatransform.extract.idc import IDC

"""
CONSTANTS
"""
endpoint = "Patient"
uuid = str(uuid4().hex)
version = "10"
out_file = f"idc.all_{endpoint}_{version}_{uuid}.jsonl.gz"
project_dataset = "gdc-bq-sample.dev"
dest_table_id = f"{project_dataset}.IDC_{endpoint}_V{version}-testing-{uuid}"
source_table = f"bigquery-public-data.idc_v{version}.dicom_pivot_v{version}"
dest_bucket = "gs://broad-cda-dev/airflow_testing"
dest_bucket_file_name = f"idc_v{version}_{endpoint}_{uuid}_*.jsonl.gz"


def tes_idc_extract():

    idc = IDC(
        bq_project_dataset=project_dataset,
        dest_table_id=dest_table_id,
        source_table=source_table,
        endpoint=endpoint,
        dest_bucket=dest_bucket,
        dest_bucket_file_name=dest_bucket_file_name,
        out_file=out_file,
    )

    print("Writing to table")
    idc.query_idc_to_table()

    print("Writing to bucket")
    idc.table_to_bucket()

    print(f"Composing bucket files into output file {out_file}")
    result = idc.download_blob()

    print(f"Completed. Resulting file: {result}")
