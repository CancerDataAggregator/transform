import json
import os
from typing import Dict, Union

from google.cloud import bigquery
from google.cloud.bigquery.dataset import DatasetReference
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Table, TableReference

try:
    from cdatransform.services.storage_service import StorageService
except ImportError:
    from dags.cdatransform.services.storage_service import StorageService


class TransformSchema:
    SCHEMA_DIR = (
        os.environ["CDA_SCHEMA_DIRECTORY"]
        if "CDA_SCHEMA_DIRECTORY" in os.environ
        else os.path.abspath("./dags/cdatransform/load/cda_schemas")
    )

    def __init__(
        self, load_result: Dict, destination_bucket: str, project: str, dataset: str
    ):
        self.client = bigquery.Client()
        """from_service_account_json('bigquery-service-account.json')"""
        self.load_result = load_result
        self.destination_bucket = destination_bucket
        self.project = project
        self.dataset = dataset
        self.dataset_ref: DatasetReference = self.client.dataset(
            self.dataset, project=self.project
        )
        self.storage_service = StorageService()

    def process_schema_definitions(self, field_configs: Dict, schema_defs, parent: str):
        def_list: list[Dict] = []

        for schema_def in schema_defs:
            new_dict = {
                "description": schema_def.description,
                "mode": schema_def.mode,
                "type": schema_def.field_type,
                "name": schema_def.name,
            }

            table_field = f"{schema_def.name}"
            if len(parent) > 0:
                table_field = f"{parent}.{table_field}"

            fields = self.process_schema_definitions(
                field_configs, schema_def.fields, table_field
            )
            if len(fields) > 0:
                new_dict["fields"] = fields

            if table_field in field_configs:
                new_dict = {
                    **new_dict,
                    **self.__replaceTableNames(field_configs[table_field]),
                }

            def_list.append(new_dict)

        return def_list

    def __replaceTableNames(self, obj):
        if isinstance(obj, dict):
            for key in obj:
                obj[key] = self.__replaceTableNames(obj[key])
        if isinstance(obj, list):
            return [self.__replaceTableNames(list_obj) for list_obj in obj]
        if isinstance(obj, str):
            return self.__replaceTableName(obj)

        return obj

    def __replaceTableName(self, text):
        if not text.endswith("Table}"):
            return text
        index = text.replace("Table", "").replace("{", "").replace("}", "")
        return self.load_result[index]

    def extract_schema_and_transform(self, schema_name: str, table_name: str):
        table_ref: TableReference = self.dataset_ref.table(table_name)
        table: Table = self.client.get_table(table_ref)

        with open(
            f"{self.SCHEMA_DIR}/{schema_name}.json", "r", encoding="utf-8"
        ) as table_file:
            table_data = json.load(table_file)

        schema_dict: Dict = {
            "tableAlias": table_data["tableAlias"],
            "definitions": self.process_schema_definitions(
                table_data["fieldConfigs"], table.schema, ""
            ),
        }

        with self.storage_service.get_session(
            f"{self.destination_bucket}/{table_name}.json", "w"
        ) as file_output:
            # with open(f"{self.SCHEMA_DIR}/{table_name}.json", "w") as file_output:
            file_output.write(
                json.dumps(
                    schema_dict, sort_keys=False, indent=4, separators=(",", ": ")
                )
            )

    def transform(self):
        for schema_name in self.load_result:
            self.extract_schema_and_transform(
                schema_name=schema_name, table_name=self.load_result[schema_name]
            )

        return True
