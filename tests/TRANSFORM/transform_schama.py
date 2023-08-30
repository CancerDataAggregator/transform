import json
from glob import glob
from unittest import mock

from dags.cdatransform.load.translate_schema import TransformSchema

# from dags.cdatransform.services.context_service import ContextService

transformed_schema_bucket = "gs://broad-cda-dev/airflow_testing/api-schema"


# for files in glob("dags/cdatransform/load/cda_schemas/*.json"):
#     print(str(json.loads(open(files).read())["tableName"]).find("Files") == -1)


def test_transform_schema():
    subject_dest_table = "all_merged_subjects_v3_4_final"
    files_dest_table = "all_merged_files_v3_4_final"
    mutation_dest_table = "somatic_mutation_hg38_gdc_r36_v3_4_final"

    load_result = {
        "Subjects": subject_dest_table,
        "Files": files_dest_table,
        "Mutations": mutation_dest_table,
    }
    TransformSchema(
        load_result=load_result,
        destination_bucket=transformed_schema_bucket,
        project="gdc-bq-sample",
        dataset="dev",
    ).transform()


test_transform_schema()
