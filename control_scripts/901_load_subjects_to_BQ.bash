#!/usr/bin/env bash

# sometimes:
# gcloud auth application-default login

gcloud config set project YOUR_PROJECT_HERE

subjects_schema=./package_root/load/schemas/subjects.schema.json

input_file=./BigQuery_JSONL/subjects.json.gz

bq load --replace=true --schema="${subjects_schema}" --source_format=NEWLINE_DELIMITED_JSON YOUR_DESTINATION_BQ_TABLE_HERE "${input_file}"


