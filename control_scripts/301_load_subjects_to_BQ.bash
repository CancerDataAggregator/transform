#!/bin/bash

# sometimes:
# gcloud auth application-default login

gcloud config set project YOUR_PROJECT_HERE

subjects_schema=./package_root/load/schemas/subjects.schema.json

input_file=./BigQuery_JSONL/subjects.json.gz

bq load --replace=true --schema="${subjects_schema}" --source_format=NEWLINE_DELIMITED_JSON dev.all_merged_subjects_v3_3_final "${input_file}"


