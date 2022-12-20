from typing import Any

from airflow.operators.python import get_current_context
from typing_extensions import TypedDict


class typed_context(TypedDict):
    project: str
    data_version: str
    dataset: str
    dag_run: Any


class ContextService:
    def __init__(self):
        self.context = get_current_context()
        self.dag_run = self.context["dag_run"]
        self.config: typed_context = self.dag_run.conf

    @property
    def version(self) -> str:
        return self.config["data_version"]

    @property
    def project(self) -> str:
        return self.config["project"]

    @property
    def dataset(self) -> str:
        return self.config["dataset"]

    def validate(self):
        if "data_version" not in self.config:
            raise ValueError("data_version is required in order to run this dag.")

        if "project" not in self.config:
            raise ValueError("project is required in order to run this dag.")

        if "dataset" not in self.config:
            raise ValueError("dataset is required in order to run this dag.")
