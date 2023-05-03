#!/bin/bash

subjects_schema=./loader_schemas/subjects.schema.json

input_file=./BigQuery_JSONL/subjects.json.gz

bq load --replace=true --schema="${subjects_schema}" --source_format=NEWLINE_DELIMITED_JSON yet_another_sandbox.all_merged_subjects "${input_file}"


