#!/usr/bin/env bash

# A CDA version label is required by the source-aware counter script.
# 
# If the length of our argument list is zero, fail.

if [[ $# -eq 0 ]]
then
    script_name=$(basename "$0")
    echo
    echo "   ERROR: Usage: $script_name <CDA version name, e.g. \"August 2024\">"
    echo
    exit 0
fi

cda_version_string=$1

chmod 755 ./package_root/auxiliary_scripts/*py

gdc_extraction_root="./extracted_data/gdc/all_TSV_output"

gdc_extraction_date_file="${gdc_extraction_root}/extraction_date.txt"

gdc_extraction_date_string=`cat ${gdc_extraction_date_file}`

gdc_version_string=`cat ${gdc_extraction_root}/API_version_metadata.files_endpoint_extraction.json | grep data_release | grep -v data_release_version | sed -E 's/^ +"data_release": "(.*)",?$/\1/' | sed -E 's/"//g'`

pdc_extraction_root="./extracted_data/pdc"

pdc_extraction_date_file="${pdc_extraction_root}/extraction_date.txt"

pdc_extraction_date_string=`cat ${pdc_extraction_date_file}`

pdc_version_string=`cat ${pdc_extraction_root}/__version_metadata/uiDataVersionSoftwareVersion.json | grep data_release | grep -v data_release_version | sed -E 's/^ +"data_release": "(.*)",?$/Data Release \1/' | sed -E 's/"//g'`

input_root=./cda_tsvs/merged_gdc_and_pdc_002_decorated_harmonized

summary_output_dir=./auxiliary_metadata/__column_value_statistics/merged_CDA_data/GDC_${gdc_extraction_date_string}_plus_PDC_${pdc_extraction_date_string}/dataset_summaries

summary_file="${summary_output_dir}/GDC_plus_PDC_column_stats_by_table.converted_to_CDA.harmonized.merged.counts_by_upstream_data_source.tsv"

current_date=$(date '+%Y-%m-%d')

echo ./package_root/auxiliary_scripts/901_summarize_distinct_values_by_table_and_column.source_aware.py $input_root $summary_file "${cda_version_string}" $current_date print_CDA_rows gdc $gdc_extraction_date_string "${gdc_version_string}" pdc $pdc_extraction_date_string "${pdc_version_string}"
./package_root/auxiliary_scripts/901_summarize_distinct_values_by_table_and_column.source_aware.py $input_root $summary_file "${cda_version_string}" $current_date print_CDA_rows gdc $gdc_extraction_date_string "${gdc_version_string}" pdc $pdc_extraction_date_string "${pdc_version_string}"

# Add a release_metadata table to $input_root by combining a curated set of CDA
# column types ('categorical', 'numeric', null) from lib.py with the profiling
# done by 901_summarize_distinct_values_by_table_and_column.source_aware.py above.

echo ./package_root/auxiliary_scripts/992_create_release_metadata_table.py $summary_file $input_root
./package_root/auxiliary_scripts/992_create_release_metadata_table.py $summary_file $input_root


