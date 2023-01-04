from unittest import mock

from dags.cdatransform.load.translate_schema import TransformSchema
from dags.cdatransform.services.context_service import ContextService

transformed_schema_bucket = "gs://broad-cda-dev/airflow_testing/api-schema"


def test_transform_schema():
    subject_dest_table = f"test_table_all_Subjects_v3_1_final"
    files_dest_table = f"all_gdc_files_arthur_dev"
    load_result = {"Subjects": subject_dest_table, "Files": files_dest_table}
    TransformSchema(
        load_result=load_result,
        destination_bucket=transformed_schema_bucket,
        project="gdc-bq-sample",
        dataset="yet_another_sandbox",
    ).transform()


test_transform_schema()
