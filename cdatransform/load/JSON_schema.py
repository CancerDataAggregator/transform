import argparse
import yaml
from yaml import Loader
import json


class Schema:
    def __init__(self, mapping, data_model_dir, outfile) -> None:
        self.outfile = outfile
        self.mapping = yaml.load(open(mapping, "r"), Loader=Loader)
        self.data_model = self._init_data_model(data_model_dir)

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

    def make_entity_schema(self, entity):
        print("Ran with entity:")
        print(entity)

        entity_dict = dict({})
        entity_dict["name"] = entity
        entity_dict["description"] = self.data_model[entity].get("description", "")
        entity_dict["type"] = "RECORD"
        entity_dict["mode"] = "REPEATED"
        entity_dict["fields"] = []
        for field in list(self.mapping[entity]["Mapping"].keys()):
            field_dict = dict({})
            field_dict["description"] = (
                self.data_model[entity].get(field, dict({})).get("description", "")
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
                field_dict["fields"] = self.add_identifier(entity)
                entity_dict["fields"].append(field_dict)
                continue
            if self.data_model[entity].get(field, dict({})).get("oneOf") is not None:
                field_dict["type"] = (
                    self.data_model[entity]
                    .get(field, dict({}))
                    .get("oneOf", [dict({"type": "string"})])[1]["type"]
                    .upper()
                )
                field_dict["mode"] = "REPEATED"
            else:
                field_dict["type"] = (
                    self.data_model[entity].get(field, dict({})).get("type", "").upper()
                )
                if (
                    field_dict["type"] == ""
                    and self.data_model[entity].get(field, dict({})).get("$ref", None)
                    is not None
                ):
                    field_dict["type"] = "STRING"
            if field in [
                "associated_project",
                "species",
                "drs_uri",
                "data_modality",
                "imaging_modality",
                "primary_disease_type",
            ]:
                field_dict["type"] = "STRING"
                field_dict["mode"] = "NULLABLE"
            if field in ["days_to_treatment_start", "days_to_treatment_end"]:
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
        return entity_dict

    def write_json_schema(self):
        print("attempted to run build json")
        full_schema = []
        Patient = self.make_entity_schema("Patient")
        RS = self.make_entity_schema("ResearchSubject")
        Diag = self.make_entity_schema("Diagnosis")
        Treat = self.make_entity_schema("Treatment")
        Spec = self.make_entity_schema("Specimen")
        File = self.make_entity_schema("File")
        Spec["fields"].append(File)
        Diag["fields"].append(Treat)
        RS["fields"].append(Diag)
        RS["fields"].append(File)
        RS["fields"].append(Spec)
        Patient["fields"].append(File)
        Patient["fields"].append(RS)
        with open(self.outfile, "w") as outfile:
            json.dump(Patient["fields"], outfile)


def main():
    parser = argparse.ArgumentParser(
        description="Generate JSON schema using mapping and cda-data-model json files."
    )
    parser.add_argument("out_file", help="Out file name. Should end with .json")
    parser.add_argument("mapping_file", help="Either GDC or PDC mapping file location")
    parser.add_argument(
        "data_model_dir",
        help="location of directory containing cda-data-model json schemas",
    )
    args = parser.parse_args()

    schema = Schema(
        mapping=args.mapping_file,
        data_model_dir=args.data_model_dir,
        outfile=args.out_file,
    )
    schema.write_json_schema()


if __name__ == "__main__":
    main()
