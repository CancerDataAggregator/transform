#!/usr/bin/env bash

# An IDC release version label is required.
# 
# If the length of our argument list is zero, fail.

if [[ $# -eq 0 ]]
then
    script_name=$(basename "$0")
    echo
    echo "   ERROR: Usage: $script_name <IDC version label e.g. v24>"
    echo
    exit 0
fi

idc_version_string=$1

chmod 755 package_root/extract/idc/scripts/*py

idc_extraction_root=./extracted_data/idc
idc_raw_extraction_dir="${idc_extraction_root}/__raw_BigQuery_JSONL"
mkdir -p $idc_raw_extraction_dir

echo $idc_version_string > "${idc_extraction_root}/data_version.txt"

current_date=$(date '+%Y-%m-%d')
echo $current_date > "${idc_extraction_root}/extraction_date.txt"

echo ./package_root/extract/idc/scripts/000_auxiliary_metadata.py
./package_root/extract/idc/scripts/000_auxiliary_metadata.py

echo ./package_root/extract/idc/scripts/001_original_collections_metadata.py
./package_root/extract/idc/scripts/001_original_collections_metadata.py

echo ./package_root/extract/idc/scripts/002_tcga_biospecimen_rel9.py
./package_root/extract/idc/scripts/002_tcga_biospecimen_rel9.py

echo ./package_root/extract/idc/scripts/003_tcga_clinical_rel9.py
./package_root/extract/idc/scripts/003_tcga_clinical_rel9.py

echo ./package_root/extract/idc/scripts/004_dicom_all.py
./package_root/extract/idc/scripts/004_dicom_all.py


