#!/bin/bash

overwrite_table="true"

files_schema=./loader_schemas/files.schema.json

for input_file in ./BigQuery_JSONL/files.*.json.gz
do
    replace_string=$overwrite_table

    if [[ $overwrite_table == "true" ]]
    then
        overwrite_table="false"
    fi

    bq load --replace="${replace_string}" --schema="${files_schema}" --source_format=NEWLINE_DELIMITED_JSON yet_another_sandbox.all_merged_files "${input_file}"
done


