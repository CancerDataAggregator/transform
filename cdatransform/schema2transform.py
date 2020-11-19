# A simple standalone script that reads in a BQ table and produces a YML file
# that can be used to write a transform specifications file

import yaml

from google.cloud import bigquery

client = bigquery.Client()

table = "gdc-bq-sample.gdc_metadata.r26_clinical_and_file"
query_job = client.query(f"SELECT * FROM {table} LIMIT 1")
result = query_job.result()

schema = result.schema

def parse_schema(_schema):
    _transforms = {"CDA-X": {}}
    if len(_schema.fields):
        _transforms["CHILD"] = {
            _field.name: parse_schema(_field)
            for _field in _schema.fields
        }
    return _transforms


transform_template = {
    v.name: parse_schema(v) for v in schema
}

yaml.dump(
    transform_template, 
    open("transform-template.yml", "w"), 
    width=50, indent=2)
