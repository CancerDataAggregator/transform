#!/usr/bin/env bash

# An IDC version string (e.g. 'v21') is required by the processor scripts.
# 
# If the length of the argument list to this script is zero, fail.

if [[ $# -eq 0 ]]
then
    script_name=$(basename "$0")
    echo
    echo "   ERROR: Usage: $script_name <IDC version string, e.g. v21>"
    echo
    exit 0
fi

version_string=$1

chmod 755 ./package_root/auxiliary_scripts/*py

echo ./package_root/auxiliary_scripts/310_count_distinct_values_by_table_and_column.IDC_CDA_data.py $version_string
./package_root/auxiliary_scripts/310_count_distinct_values_by_table_and_column.IDC_CDA_data.py $version_string

echo ./package_root/auxiliary_scripts/311_count_distinct_values_by_table_and_column.merged_IDC_PDC_and_GDC_CDA_data.py
./package_root/auxiliary_scripts/311_count_distinct_values_by_table_and_column.merged_IDC_PDC_and_GDC_CDA_data.py

echo ./package_root/auxiliary_scripts/320_tabulate_enumerable_values_by_table_and_column.all_extracted_IDC_data.py $version_string
./package_root/auxiliary_scripts/320_tabulate_enumerable_values_by_table_and_column.all_extracted_IDC_data.py $version_string


