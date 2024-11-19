#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*py

data_source=pdc

upper_data_source=$(echo "$data_source" | tr '[:lower:]' '[:upper:]')

extraction_root="./extracted_data/${data_source}"

extraction_date_file="${extraction_root}/extraction_date.txt"

extraction_date_string=`cat $extraction_date_file`

version_string=`cat ${extraction_root}/__version_metadata/uiDataVersionSoftwareVersion.json | grep data_release | grep -v data_release_version | sed -E 's/^ +"data_release": "(.*)",?$/Data Release \1/' | sed -E 's/"//g'`

aux_dir="./auxiliary_metadata/__column_value_statistics/${upper_data_source}/extracted__${extraction_date_string}"

full_count_output_root="${aux_dir}/full_counts_by_column_and_value"

summary_output_dir="${aux_dir}/dataset_summaries"

summary_file="${summary_output_dir}/${upper_data_source}_column_stats_by_table.as_extracted.fragmented_uncollated_redundant.tsv"

echo ./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $extraction_root $summary_file
./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $extraction_root $summary_file

input_root="./extracted_data/${data_source}_postprocessed"

summary_file="${summary_output_dir}/${upper_data_source}_column_stats_by_table.as_extracted.QCd_collated_nonredundant.tsv"

echo ./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file
./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file

# Scrape and document a complete set of unique values and counts for all enumerable
# extracted columns. ("Enumerable" generally includes everything except ID fields
# and numeric fields; exceptions include year values, some discrete clinical
# numeric values with small value sets like "alcohol_days_per_week", or conceptually
# unbounded but still manageably finite values like dbGaP study accessions. An enumerable
# field list is defined for each data source in the columns_to_count() function in /lib.py.)

full_count_output_dir="${full_count_output_root}/000_extracted_QCd_collated_nonredundant"

echo ./package_root/auxiliary_scripts/902_tabulate_enumerable_values_by_table_and_column.py $data_source $input_root $full_count_output_dir
./package_root/auxiliary_scripts/902_tabulate_enumerable_values_by_table_and_column.py $data_source $input_root $full_count_output_dir

input_root="./cda_tsvs/${data_source}_000_unharmonized"

summary_file="${summary_output_dir}/${upper_data_source}_column_stats_by_table.converted_to_CDA.unharmonized.tsv"

echo ./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file
./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file

full_count_output_dir="${full_count_output_root}/001_converted_to_CDA.unharmonized"

# Switch data source to 'cda' to identify the right columns to enumerate for the new format.

echo ./package_root/auxiliary_scripts/902_tabulate_enumerable_values_by_table_and_column.py cda $input_root $full_count_output_dir
./package_root/auxiliary_scripts/902_tabulate_enumerable_values_by_table_and_column.py cda $input_root $full_count_output_dir

input_root="./cda_tsvs/${data_source}_001_harmonized"

summary_file="${summary_output_dir}/${upper_data_source}_column_stats_by_table.converted_to_CDA.harmonized.tsv"

echo ./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file
./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file

full_count_output_dir="${full_count_output_root}/002_converted_to_CDA.harmonized"

# Switch data source to 'cda' to identify the right columns to enumerate for the new format.

echo ./package_root/auxiliary_scripts/902_tabulate_enumerable_values_by_table_and_column.py cda $input_root $full_count_output_dir
./package_root/auxiliary_scripts/902_tabulate_enumerable_values_by_table_and_column.py cda $input_root $full_count_output_dir

input_root="./cda_tsvs/${data_source}_002_decorated_harmonized"

summary_file="${summary_output_dir}/${upper_data_source}_column_stats_by_table.converted_to_CDA.harmonized.aliased.tsv"

echo ./package_root/auxiliary_scripts/901_summarize_distinct_values_by_table_and_column.source_aware.py $input_root $summary_file NULL_VERSION NULL_DATE suppress_CDA_rows $data_source $extraction_date_string "$version_string"
./package_root/auxiliary_scripts/901_summarize_distinct_values_by_table_and_column.source_aware.py $input_root $summary_file NULL_VERSION NULL_DATE suppress_CDA_rows $data_source $extraction_date_string "$version_string"

# Add a release_metadata table to cda_tsvs/${data_source}_002_decorated_harmonized/ by combining a
# curated set of CDA column types ('categorical', 'numeric', null) from lib.py with
# the profiling done by 901_summarize_distinct_values_by_table_and_column.source_aware.py above.

echo ./package_root/auxiliary_scripts/992_create_release_metadata_table.py $summary_file $input_root
./package_root/auxiliary_scripts/992_create_release_metadata_table.py $summary_file $input_root


