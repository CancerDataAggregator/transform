#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*py

# Count values in various dataset versions.

echo ./package_root/auxiliary_scripts/000_list_GDC_entities_by_program_and_project.py
./package_root/auxiliary_scripts/000_list_GDC_entities_by_program_and_project.py

echo ./package_root/auxiliary_scripts/001_enumerate_GDC_program_project_hierarchy.py
./package_root/auxiliary_scripts/001_enumerate_GDC_program_project_hierarchy.py

extraction_root=./extracted_data/gdc/all_TSV_output

extraction_date_file="${extraction_root}/extraction_date.txt"

extraction_date_string=`cat $extraction_date_file`

summary_output_dir=./auxiliary_metadata/__column_value_statistics/GDC/extracted__${extraction_date_string}/dataset_summaries

summary_file="${summary_output_dir}/GDC_column_stats_by_table.as_extracted.tsv"

echo ./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $extraction_root $summary_file
./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $extraction_root $summary_file

# Scrape and document a complete set of unique values and counts for all enumerable
# extracted columns. ("Enumerable" generally includes everything except ID fields
# and numeric fields; exceptions include year values, some discrete clinical
# numeric values with small value sets like "alcohol_days_per_week", or conceptually
# unbounded but still manageably finite values like dbGaP study accessions. An enumerable
# field list is defined for each data source in the columns_to_count() function in /lib.py.)

input_root=./extracted_data/gdc/all_TSV_output

full_count_output_dir=./auxiliary_metadata/__column_value_statistics/GDC/extracted__${extraction_date_string}/full_counts_by_column_and_value

data_source=gdc

echo ./package_root/auxiliary_scripts/902_tabulate_enumerable_values_by_table_and_column.py $data_source $input_root $full_count_output_dir
./package_root/auxiliary_scripts/902_tabulate_enumerable_values_by_table_and_column.py $data_source $input_root $full_count_output_dir

input_root=./cda_tsvs/gdc_000_unharmonized

summary_file="${summary_output_dir}/GDC_column_stats_by_table.converted_to_CDA.unharmonized.tsv"

echo ./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file
./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file

input_root=./cda_tsvs/gdc_001_harmonized

summary_file="${summary_output_dir}/GDC_column_stats_by_table.converted_to_CDA.harmonized.tsv"

echo ./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file
./package_root/auxiliary_scripts/900_summarize_distinct_values_by_table_and_column.py $input_root $summary_file

# The postprocessed output of the following script does not go into CDA public releases; we prepare
# it upstream of ISB-CGC's analytic consumption of the data as a courtesy, but responsibility
# for this level of curation rests firmly with the CRDC data centers and not with CDA.

echo ./package_root/auxiliary_scripts/010_rewire_and_fix_GDC_subsample_provenance.py
./package_root/auxiliary_scripts/010_rewire_and_fix_GDC_subsample_provenance.py

# Add a release_metadata table to cda_tsvs/gdc_002_decorated_harmonized/ by combining a
# curated set of CDA column types ('categorical', 'numeric', null) from lib.py with
# the profiling done by 900_summarize_distinct_values_by_table_and_column.py above.

version_string=`cat ${extraction_root}/API_version_metadata.files_endpoint_extraction.json | grep data_release | sed -E 's/^ +"data_release": "(.*)",?$/\1/' | sed -E 's/"//g'`

echo ./package_root/auxiliary_scripts/995_create_single-DC_release_metadata_table.py $data_source "$version_string" $extraction_date_string $summary_file ./cda_tsvs/gdc_002_decorated_harmonized
./package_root/auxiliary_scripts/995_create_single-DC_release_metadata_table.py $data_source "$version_string" $extraction_date_string $summary_file ./cda_tsvs/gdc_002_decorated_harmonized


