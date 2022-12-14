import json
from functools import lru_cache

import yaml
from yaml import Loader

try:
    from cdatransform.lib import yamlPathMapping
    from cdatransform.services.storage_service import StorageService
except ImportError:
    from dags.cdatransform.lib import yamlPathMapping
    from dags.cdatransform.services.storage_service import StorageService

from .download_json_schema import make_request, url_download


class Schema:
    def __init__(
        self,
        mapping,
        outfile,
        endpoint,
        missing_descriptions_file="cdatransform/load/missing_descriptions.json",
        ccdh_mapping_file="cdatransform/load/ccdh_map.json",
    ) -> None:
        self.outfile = outfile
        self.endpoint = endpoint
        self.ccdh_mapping_file = ccdh_mapping_file
        self.mapping = yaml.load(open(yamlPathMapping(mapping), "r"), Loader=Loader)
        self.ccdh_mapping = self._init_ccdh_model(ccdh_mapping_file)
        self.missing_descriptions = self._init_missing_descriptions(
            missing_descriptions_file
        )
        self.data_model = self._init_data_model()

    def _init_ccdh_model(self, ccdh_mapping_file):
        return_dict = {}
        with open(ccdh_mapping_file, "r") as f:
            ccdh_mapping = json.load(f)
        # MandT = yaml.load(open(cda_mapping_yaml, "r"), Loader=Loader)
        for entity, MorT_dict in self.mapping.items():
            # return_dict[entity.replace('_merge','')] = MorT_dict['Mapping']
            return_dict[entity] = {}
            for field in MorT_dict["Mapping"]:
                if field in ccdh_mapping.get(entity, {}):
                    return_dict[entity][field] = ccdh_mapping[entity][field]
                else:
                    return_dict[entity][field] = field
        print(return_dict)
        return return_dict

    def _init_missing_descriptions(self, missing_descriptions_file):
        with open(missing_descriptions_file, "r") as f:
            return json.load(f)

    @lru_cache()
    def _init_data_model(self):
        print("ran _init_data_model")
        data_model = dict({})
        for entity, data in url_download(list(self.mapping.keys())):
            data_model[entity] = data["properties"]
            data_model[entity]["description"] = data["description"]
        data = make_request("definitions")
        data_model["identifier"] = data["definitions"]["Identifier"]["properties"]
        return data_model

    def add_identifier(self, entity):
        fields = []
        for field in list(self.mapping[entity]["Mapping"]["identifier"].keys()):
            field_dict = dict({})
            field_dict["description"] = self.data_model["identifier"][field][
                "description"
            ]
            field_dict["name"] = field
            field_dict["type"] = "STRING"
            fields.append(field_dict)
        return fields

    def add_linkers(self, entity):
        fields = []
        for field in list(self.mapping[entity]["Linkers"].keys()):
            field_dict = {}
            field_dict["description"] = " ".join(
                ["List of ids of", field[:-1], "entities associated with the", entity]
            )
            field_dict["description"] + "."
            field_dict["name"] = field
            field_dict["type"] = "STRING"
            field_dict["mode"] = "REPEATED"
            fields.append(field_dict)
        return fields

    def make_entity_schema(self, entity):
        print("Ran with entity:")
        print(entity)
        entity_dict = {}
        entity_dict["name"] = entity
        # ccdh_map
        if entity == "Subject":
            entity_dict["description"] = self.data_model["Patient"].get(
                "description", ""
            )
            entity_loop = "Patient"
            # ccdh_map = self.ccdh_mapping['Patient']
        else:
            print(self.data_model[entity])
            entity_dict["description"] = self.data_model[entity].get("description", "")
            entity_loop = entity
        entity_dict["type"] = "RECORD"
        entity_dict["mode"] = "REPEATED"
        entity_dict["fields"] = []

        for field in list(self.ccdh_mapping[entity_loop].keys()):
            field_dict = {}
            field_dict["description"] = (
                self.data_model[entity_loop]
                .get(self.ccdh_mapping[entity_loop][field], {})
                .get("description", "")
            )
            field_dict["name"] = field
            if field == "id":
                field_dict["mode"] = "REQUIRED"
                field_dict["type"] = "STRING"
                entity_dict["fields"].append(field_dict)
                continue
            if field == "identifier":
                field_dict["type"] = "RECORD"
                field_dict["mode"] = "REPEATED"
                field_dict["fields"] = self.add_identifier(entity_loop)
                entity_dict["fields"].append(field_dict)
                continue
            if (
                self.data_model[entity_loop]
                .get(self.ccdh_mapping[entity_loop][field], {})
                .get("oneOf")
                is not None
            ):
                field_dict["type"] = (
                    self.data_model[entity_loop]
                    .get(self.ccdh_mapping[entity_loop][field], {})
                    .get("oneOf", [dict({"type": "string"})])[1]["type"]
                    .upper()
                )
                field_dict["mode"] = "REPEATED"
            else:
                field_dict["type"] = (
                    self.data_model[entity_loop]
                    .get(self.ccdh_mapping[entity_loop][field], {})
                    .get("type", "")
                    .upper()
                )
                if (
                    field_dict["type"] == ""
                    and self.data_model[entity_loop]
                    .get(self.ccdh_mapping[entity_loop][field], {})
                    .get("$ref", None)
                    is not None
                ):
                    field_dict["type"] = "STRING"
            if field in self.missing_descriptions.get(entity_loop, {}):
                field_dict["description"] = self.missing_descriptions[entity_loop][
                    field
                ]
            if field in [
                "species",
                "drs_uri",
                "data_modality",
                "imaging_modality",
                "primary_disease_type",
                "member_of_research_project",
                "vital_status",
                "cause_of_death",
                "primary_diagnosis_condition",
                "primary_diagnosis_site",
                "dbgap_accession_number",
                "method_of_diagnosis",
                "therapeutic_agent",
                "treatment_anatomic_site",
                "treatment_effect",
                "treatment_end_reason",
                "associated_project",
                "imaging_series",
            ]:
                field_dict["type"] = "STRING"
                field_dict["mode"] = "NULLABLE"
            if field in [
                "days_to_treatment_start",
                "days_to_treatment_end",
                "days_to_death",
                "days_to_collection",
                "number_of_cycles",
            ]:
                field_dict["type"] = "INTEGER"
                field_dict["mode"] = "NULLABLE"
            if field == "subject_associated_project":
                field_dict["type"] = "STRING"
                field_dict["mode"] = "REPEATED"
            if field == "derived_from_specimen":
                field_dict["type"] = "STRING"
                field_dict["mode"] = "NULLABLE"
            if field_dict["type"].lower() == "number":
                field_dict["type"] = "INTEGER"
            entity_dict["fields"].append(field_dict)
        if self.mapping[entity_loop].get("Linkers"):
            entity_dict["fields"].extend(self.add_linkers(entity_loop))
        return entity_dict

    def write_json_schema(self):
        print("attempted to run build json")
        if self.endpoint == "Patient":
            # Patient table procedure
            Patient = self.make_entity_schema("Subject")
            RS = self.make_entity_schema("ResearchSubject")
            Diag = self.make_entity_schema("Diagnosis")
            Treat = self.make_entity_schema("Treatment")
            Spec = self.make_entity_schema("Specimen")
            Diag["fields"].append(Treat)
            RS["fields"].append(Diag)
            RS["fields"].append(Spec)
            Patient["fields"].append(RS)
            with StorageService().get_session(self.outfile, "w") as outfile:
                json.dump(Patient["fields"], outfile, indent=4)
        # File table procedure
        elif self.endpoint == "File":
            File = self.make_entity_schema("File")
            Patient = self.make_entity_schema("Subject")
            RS = self.make_entity_schema("ResearchSubject")
            Diag = self.make_entity_schema("Diagnosis")
            Treat = self.make_entity_schema("Treatment")
            Spec = self.make_entity_schema("Specimen")
            Diag["fields"].append(Treat)
            RS["fields"].append(Diag)
            File["fields"].extend([Patient, RS, Spec])
            with StorageService().get_session(self.outfile, "w") as outfile:
                outfile.write(
                    json.dumps(
                        File["fields"],
                        indent=4,
                    )
                )
