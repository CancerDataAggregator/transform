from airflow.operators.python import get_current_context


class ContextService:
    def __init__(self):
        self.context = get_current_context()
        self.dag_run = self.context["dag_run"]
        self.config = self.dag_run.conf

    @property
    def version(self):
        return self.config["data_version"]

    @property
    def project(self):
        return self.config["project"]

    @property
    def dataset(self):
        return self.config["dataset"]

    def validate(self):
        if "data_version" not in self.config:
            raise ValueError("data_version is required in order to run this dag.")

        if "project" not in self.config:
            raise ValueError("project is required in order to run this dag.")

        if "dataset" not in self.config:
            raise ValueError("dataset is required in order to run this dag.")
