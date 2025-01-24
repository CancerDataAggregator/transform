#!/usr/bin/env bash

# An IDC version string (e.g. 'v20') is required by the processor scripts.
# 
# If the length of the argument list to this script is zero, fail.

if [[ $# -eq 0 ]]
then
    script_name=$(basename "$0")
    echo
    echo "   ERROR: Usage: $script_name <IDC version string, e.g. v20>"
    echo
    exit 0
fi

version_string=$1

chmod 755 ./199_idc_controllers/*py ./package_root/auxiliary_scripts/*py

echo ./199_idc_controllers/010_extract_submitter_IDs_from_auxiliary_metadata_to_tsv.py $version_string
./199_idc_controllers/010_extract_submitter_IDs_from_auxiliary_metadata_to_tsv.py $version_string

echo ./199_idc_controllers/011_extract_file_subject_and_rs_metadata_to_tsv.py $version_string
./199_idc_controllers/011_extract_file_subject_and_rs_metadata_to_tsv.py $version_string

echo ./199_idc_controllers/012_map_case_ids_to_collection_ids.py $version_string
./199_idc_controllers/012_map_case_ids_to_collection_ids.py $version_string

echo ./199_idc_controllers/014_populate_rs_primary_diagnosis_condition.py $version_string
./199_idc_controllers/014_populate_rs_primary_diagnosis_condition.py $version_string

echo ./199_idc_controllers/015_populate_tcga_clinical_subject_metadata.py $version_string
./199_idc_controllers/015_populate_tcga_clinical_subject_metadata.py $version_string

echo ./199_idc_controllers/016_populate_tcga_biospecimen_metadata.py $version_string
./199_idc_controllers/016_populate_tcga_biospecimen_metadata.py $version_string

echo ./package_root/auxiliary_scripts/999_harmonize_cda_tsvs.py ./cda_tsvs/idc_raw_unharmonized/$version_string ./cda_tsvs/idc/$version_string ./auxiliary_metadata/__substitution_logs/idc
./package_root/auxiliary_scripts/999_harmonize_cda_tsvs.py ./cda_tsvs/idc_raw_unharmonized/$version_string ./cda_tsvs/idc/$version_string ./auxiliary_metadata/__substitution_logs/idc


