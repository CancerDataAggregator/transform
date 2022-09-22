import argparse
import json

import yaml
from yaml import Loader


class Schema:
    def __init__(
        self,
        mapping,
        ccdh_mapping_file,
        missing_descriptions_file,
        data_model_dir,
        outfile,
        endpoint,
    ) -> None:
        self.outfile = outfile
        self.endpoint = endpoint
        self.ccdh_mapping_file = ccdh_mapping_file
        self.mapping = yaml.load(open(mapping, "r"), Loader=Loader)
        self.ccdh_mapping = self._init_ccdh_model(ccdh_mapping_file)
        self.missing_descriptions = self._init_missing_descriptions(
            missing_descriptions_file
        )
        self.data_model = self._init_data_model(data_model_dir)

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

    def _init_data_model(self, data_model_dir):
        print("ran _init_data_model")
        data_model = dict({})
        for entity in list(self.mapping.keys()):
            f = open(
                data_model_dir + "/" + entity + ".json",
            )
            data = json.load(f)
            data_model[entity] = data["properties"]
            data_model[entity]["description"] = data["description"]
        f = open(data_model_dir + "/definitions.json")
        data = json.load(f)
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
            field_dict = dict({})
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
        entity_dict = dict({})
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

        entity_loop
        for field in list(self.ccdh_mapping[entity_loop].keys()):
            field_dict = dict({})
            field_dict["description"] = (
                self.data_model[entity_loop]
                .get(self.ccdh_mapping[entity_loop][field], dict({}))
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
                .get(self.ccdh_mapping[entity_loop][field], dict({}))
                .get("oneOf")
                is not None
            ):
                field_dict["type"] = (
                    self.data_model[entity_loop]
                    .get(self.ccdh_mapping[entity_loop][field], dict({}))
                    .get("oneOf", [dict({"type": "string"})])[1]["type"]
                    .upper()
                )
                field_dict["mode"] = "REPEATED"
            else:
                field_dict["type"] = (
                    self.data_model[entity_loop]
                    .get(self.ccdh_mapping[entity_loop][field], dict({}))
                    .get("type", "")
                    .upper()
                )
                if (
                    field_dict["type"] == ""
                    and self.data_model[entity_loop]
                    .get(self.ccdh_mapping[entity_loop][field], dict({}))
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
            with open(self.outfile, "w") as outfile:
                json.dump(Patient["fields"], outfile)
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
            with open(self.outfile, "w") as outfile:
                json.dump(File["fields"], outfile)


def main():
    parser = argparse.ArgumentParser(
        description="Generate JSON schema using mapping and cda-data-model json files."
    )
    parser.add_argument("out_file", help="Out file name. Should end with .json")
    parser.add_argument("mapping_file", help="Either GDC or PDC mapping file location")
    parser.add_argument(
        "ccdh_mapping_file", help="Either GDC or PDC mapping file location"
    )
    parser.add_argument(
        "missing_descriptions_file",
        help="File of descriptions not in cda-data-model files",
    )
    parser.add_argument(
        "data_model_dir",
        help="location of directory containing cda-data-model json schemas",
    )
    parser.add_argument("endpoint", help="Either File or Patient")
    args = parser.parse_args()

    schema = Schema(
        mapping=args.mapping_file,
        ccdh_mapping_file=args.ccdh_mapping_file,
        missing_descriptions_file=args.missing_descriptions_file,
        data_model_dir=args.data_model_dir,
        outfile=args.out_file,
        endpoint=args.endpoint,
    )
    schema.write_json_schema()


if __name__ == "__main__":
    main()
