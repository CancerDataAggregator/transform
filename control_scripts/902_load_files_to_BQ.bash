#!/bin/bash

# sometimes:
# gcloud auth application-default login

gcloud config set project YOUR_PROJECT_HERE

overwrite_table="true"

files_schema=./package_root/load/schemas/files.schema.json

for input_file in ./BigQuery_JSONL/files.*.json.gz
do
    replace_string=$overwrite_table

    if [[ $overwrite_table == "true" ]]
    then
        overwrite_table="false"
    fi

    bq load --replace="${replace_string}" --schema="${files_schema}" --source_format=NEWLINE_DELIMITED_JSON dev.all_merged_files_v3_3_final "${input_file}"
done


