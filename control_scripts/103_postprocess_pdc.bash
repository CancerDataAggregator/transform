#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*py

extraction_root=./extracted_data/pdc

extraction_date_file="${extraction_root}/extraction_date.txt"

extraction_date_string=`cat $extraction_date_file`

summary_output_dir=./auxiliary_metadata/__column_value_statistics/PDC/extracted__${extraction_date_string}/dataset_summaries

summary_file="${summary_output_dir}/PDC_column_stats_by_table.as_extracted.fragmented_uncollated_redundant.tsv"

echo ./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $extraction_root $summary_file
./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $extraction_root $summary_file

input_root=./extracted_data/pdc_postprocessed

summary_file="${summary_output_dir}/PDC_column_stats_by_table.as_extracted.QCd_collated_nonredundant.tsv"

echo ./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file
./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file

# Scrape and document a complete set of unique values and counts for all enumerable
# extracted columns. ("Enumerable" generally includes everything except ID fields
# and numeric fields; exceptions include year values, some discrete clinical
# numeric values with small value sets like "alcohol_days_per_week", or conceptually
# unbounded but still manageably finite values like dbGaP study accessions.)

input_root=./extracted_data/pdc_postprocessed

full_count_output_dir=./auxiliary_metadata/__column_value_statistics/PDC/extracted__${extraction_date_string}/full_counts_by_column_and_value

data_source=pdc

echo ./package_root/auxiliary_scripts/902_tabulate_enumerable_values_by_table_and_column.py $data_source $input_root $full_count_output_dir
./package_root/auxiliary_scripts/902_tabulate_enumerable_values_by_table_and_column.py $data_source $input_root $full_count_output_dir

input_root=./cda_tsvs/pdc_000_unharmonized

summary_file="${summary_output_dir}/PDC_column_stats_by_table.converted_to_CDA.unharmonized.tsv"

echo ./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file
./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file

input_root=./cda_tsvs/pdc_001_harmonized

summary_file="${summary_output_dir}/PDC_column_stats_by_table.converted_to_CDA.harmonized.tsv"

echo ./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file
./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file

# Add a release_metadata table to cda_tsvs/pdc_002_decorated_harmonized/ by combining a
# curated set of CDA column types ('categorical', 'numeric', null) from lib.py with
# the profiling done by 900_summarize_distinct_values_by_table_and_column.py above.

output_dir=./cda_tsvs/pdc_002_decorated_harmonized

version_string=`cat ./extracted_data/pdc/__version_metadata/uiDataVersionSoftwareVersion.json | grep data_release | sed -E 's/^ +"data_release": "(.*)",?$/Data Release \1/' | sed -E 's/"//g'`

echo ./package_root/auxiliary_scripts/995_create_single-DC_release_metadata_table.py $data_source "$version_string" $extraction_date_string $summary_file $output_dir
./package_root/auxiliary_scripts/995_create_single-DC_release_metadata_table.py $data_source "$version_string" $extraction_date_string $summary_file $output_dir


