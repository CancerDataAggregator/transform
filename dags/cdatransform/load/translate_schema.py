import json
from ensurepip import version
from typing import Dict

from google.cloud import bigquery
from google.cloud.bigquery.dataset import DatasetReference
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Table, TableReference


class TransformSchema:
    def __init__(self, load_result: Dict, destination_bucket):
        self.client = bigquery.Client()
        """from_service_account_json('bigquery-service-account.json')"""
        self.load_result = load_result
        self.destination_bucket = destination_bucket

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
                new_dict = {**new_dict, **field_configs[table_field]}

            def_list.append(new_dict)

        return def_list

    def extract_schema_and_transform(self, table_name: str, project: str, dataset: str):
        dataset_ref: DatasetReference = self.client.dataset(dataset, project=project)
        table_ref: TableReference = dataset_ref.table(table_name)
        table: Table = self.client.get_table(table_ref)

        with open(f"{self.version}/{table_name}.json", "r") as table_file:
            table_data = json.load(table_file)

        schema_dict: Dict = {
            "tableAlias": table_data["tableAlias"],
            "definitions": self.process_schema_definitions(
                table_data["fieldConfigs"], table.schema, ""
            ),
        }

        with open(f"{table_name}.json", "w") as file_output:
            file_output.write(
                json.dumps(
                    schema_dict, sort_keys=False, indent=4, separators=(",", ": ")
                )
            )

    def transform_by_version(self):
        if len(self.version) == 0:
            raise "Must provide a version"

        with open(f"{self.version}/versionConfig.json") as version_config:
            version_info = json.load(version_config)
            project = version_info["project"]
            dataset = version_info["dataset"]

            for table_name in version_info["tables"]:
                self.extract_schema_and_transform(table_name, project, dataset)


# if __name__ == "__main__":
#     TransformSchema("3_1").transform_by_version()
