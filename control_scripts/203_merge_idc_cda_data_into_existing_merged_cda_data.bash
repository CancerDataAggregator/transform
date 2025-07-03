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

chmod 755 ./199_idc_controllers/*py

echo ./199_idc_controllers/030_map_IDC_CDA_subject_IDs_to_other_DC_CDA_subject_ids.py $version_string
./199_idc_controllers/030_map_IDC_CDA_subject_IDs_to_other_DC_CDA_subject_ids.py $version_string

echo ./199_idc_controllers/031_map_IDC_CDA_tables_into_other_DC_CDA_tables.py $version_string
./199_idc_controllers/031_map_IDC_CDA_tables_into_other_DC_CDA_tables.py $version_string


