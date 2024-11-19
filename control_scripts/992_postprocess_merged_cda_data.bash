#!/usr/bin/env bash

# A CDA version label is required by the source-aware counter script.
# 
# If the length of our argument list is zero, fail.

if [[ $# -eq 0 ]]
then
    script_name=$(basename "$0")
    echo
    echo "   ERROR: Usage: $script_name <CDA version name, e.g. \"September 2024\">"
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

cds_extraction_date_file=./extracted_data/cds_initial_database_dump/cds_extraction_date.txt

cds_extraction_date_string=`cat $cds_extraction_date_file`

cds_version_file=./extracted_data/cds_initial_database_dump/cds_version.txt

cds_version_string=`cat $cds_version_file`

icdc_extraction_root="./extracted_data/icdc"

icdc_extraction_date_file="${icdc_extraction_root}/extraction_date.txt"

icdc_extraction_date_string=`cat ${icdc_extraction_date_file}`

icdc_version_file="${icdc_extraction_root}/data_version.txt"

icdc_version_string=`cat ${icdc_version_file}`

idc_extraction_date_file=./extracted_data/idc/extraction_date.txt

idc_extraction_date_string=`cat $idc_extraction_date_file`

idc_version_file=./extracted_data/idc/data_version.txt

idc_version_string=`cat $idc_version_file`

input_root=./cda_tsvs/last_merge

summary_output_dir=./auxiliary_metadata/__column_value_statistics/merged_CDA_data/GDC_${gdc_extraction_date_string}_plus_PDC_${pdc_extraction_date_string}_plus_CDS_${cds_extraction_date_string}_plus_ICDC_${icdc_extraction_date_string}_plus_IDC_${idc_extraction_date_string}/dataset_summaries

summary_file="${summary_output_dir}/GDC_plus_PDC_plus_CDS_plus_ICDC_plus_IDC.converted_to_CDA.harmonized.merged.counts_by_upstream_data_source.tsv"

current_date=$(date '+%Y-%m-%d')

echo ./package_root/auxiliary_scripts/901_summarize_distinct_values_by_table_and_column.source_aware.py $input_root $summary_file "${cda_version_string}" $current_date print_CDA_rows gdc $gdc_extraction_date_string "${gdc_version_string}" pdc $pdc_extraction_date_string "${pdc_version_string}" cds $cds_extraction_date_string "${cds_version_string}" icdc $icdc_extraction_date_string "${icdc_version_string}" idc $idc_extraction_date_string "${idc_version_string}"
./package_root/auxiliary_scripts/901_summarize_distinct_values_by_table_and_column.source_aware.py $input_root $summary_file "${cda_version_string}" $current_date print_CDA_rows gdc $gdc_extraction_date_string "${gdc_version_string}" pdc $pdc_extraction_date_string "${pdc_version_string}" cds $cds_extraction_date_string "${cds_version_string}" icdc $icdc_extraction_date_string "${icdc_version_string}" idc $idc_extraction_date_string "${idc_version_string}"

full_count_output_dir=./auxiliary_metadata/__column_value_statistics/merged_CDA_data/GDC_${gdc_extraction_date_string}_plus_PDC_${pdc_extraction_date_string}_plus_CDS_${cds_extraction_date_string}_plus_ICDC_${icdc_extraction_date_string}_plus_IDC_${idc_extraction_date_string}/full_counts_by_column_and_value

echo ./package_root/auxiliary_scripts/902_tabulate_enumerable_values_by_table_and_column.py cda $input_root $full_count_output_dir
./package_root/auxiliary_scripts/902_tabulate_enumerable_values_by_table_and_column.py cda $input_root $full_count_output_dir

# Add a release_metadata table to $input_root by combining a curated set of CDA
# column types ('categorical', 'numeric', null) from lib.py with the profiling
# done by 901_summarize_distinct_values_by_table_and_column.source_aware.py above.

echo ./package_root/auxiliary_scripts/992_create_release_metadata_table.py $summary_file $input_root
./package_root/auxiliary_scripts/992_create_release_metadata_table.py $summary_file $input_root


