#!/usr/bin/env bash

# An IDC version string (e.g. 'v18') is required by the processor scripts.
# 
# If the length of the argument list to this script is zero, fail.

if [[ $# -eq 0 ]]
then
    script_name=$(basename "$0")
    echo
    echo "   ERROR: Usage: $script_name <IDC version string, e.g. v18>"
    echo
    exit 0
fi

version_string=$1

chmod 755 ./199_idc_controllers/*py

echo ==================================================================================================
echo BEGIN ./199_idc_controllers/000_idc_query_auxiliary_metadata_to_table_then_bucket_then_download.py $version_string
echo ==================================================================================================
echo
./199_idc_controllers/000_idc_query_auxiliary_metadata_to_table_then_bucket_then_download.py $version_string

echo ==================================================================================================
echo BEGIN ./199_idc_controllers/001_idc_query_original_collections_metadata_to_table_then_bucket_then_download.py $version_string
echo ==================================================================================================
echo
./199_idc_controllers/001_idc_query_original_collections_metadata_to_table_then_bucket_then_download.py $version_string

echo ==================================================================================================
echo BEGIN ./199_idc_controllers/002_idc_query_tcga_biospecimen_rel9_to_table_then_bucket_then_download.py $version_string
echo ==================================================================================================
echo
./199_idc_controllers/002_idc_query_tcga_biospecimen_rel9_to_table_then_bucket_then_download.py $version_string

echo ==================================================================================================
echo BEGIN ./199_idc_controllers/003_idc_query_tcga_clinical_rel9_to_table_then_bucket_then_download.py $version_string
echo ==================================================================================================
echo
./199_idc_controllers/003_idc_query_tcga_clinical_rel9_to_table_then_bucket_then_download.py $version_string

echo ==================================================================================================
echo BEGIN ./199_idc_controllers/004_idc_query_dicom_all_to_table_then_bucket_then_download.py $version_string
echo ==================================================================================================
echo
./199_idc_controllers/004_idc_query_dicom_all_to_table_then_bucket_then_download.py $version_string


