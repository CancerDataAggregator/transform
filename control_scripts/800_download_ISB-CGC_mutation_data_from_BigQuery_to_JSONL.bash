#!/usr/bin/env bash

# A n ISB-CGC-issued data release version label is required.
# 
# If the length of our argument list is zero, fail.

if [[ $# -eq 0 ]]
then
    script_name=$(basename "$0")
    echo
    echo "   ERROR: Usage: $script_name <ISB-CGC data release version, e.g. hg38_gdc_current>"
    echo
    exit 0
fi

mutation_version_string=$1

extraction_root=./extracted_data/mutation
mkdir -p $extraction_root
version_file="${extraction_root}/source_data_version.txt"
echo "${mutation_version_string}" > $version_file

current_date=$(date '+%Y-%m-%d')
extraction_date_file="${extraction_root}/extraction_date.txt"
echo $current_date > $extraction_date_file

chmod 755 ./package_root/extract/mutation/*py

source_datasets=(
    "BEATAML1_0"
    "CDDP_EAGLE"
    "CGCI"
    "CMI"
    "CPTAC"
    "EXC_RESPONDERS"
    "HCMI"
    "MMRF"
    "TARGET"
    "TCGA"
)

source_project="isb-cgc-bq"

for source_dataset in "${source_datasets[@]}"
do
    source_table_path="${source_project}.${source_dataset}.masked_somatic_mutation_${mutation_version_string}"
    ./package_root/extract/mutation/extract_one_table.py "$source_table_path"
done


