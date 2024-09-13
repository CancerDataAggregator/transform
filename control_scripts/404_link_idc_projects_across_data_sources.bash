#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*p[yl]

################################################################################
# List IDC entities and project affiliations (generate ${idc_entity_list},
# below) and enumerate the IDC program-collection hierarchy.
################################################################################

idc_program_collection_source_file=extracted_data/idc/original_collections_metadata.tsv

idc_aux_supplemental_data_dir=./auxiliary_metadata/__IDC_supplemental_metadata

mkdir -p $idc_aux_supplemental_data_dir

idc_program_collection_target_file="${idc_aux_supplemental_data_dir}/IDC_all_programs_and_collections.tsv"

ln -sf ../../$idc_program_collection_source_file $idc_program_collection_target_file

echo ./package_root/auxiliary_scripts/410_list_IDC_entities_by_program_and_collection.py
./package_root/auxiliary_scripts/410_list_IDC_entities_by_program_and_collection.py

################################################################################
# Draft links from IDC projects to GDC projects based on entities of
# corresponding types sharing a submitter ID.
################################################################################

idc_entity_list="${idc_aux_supplemental_data_dir}/IDC_entities_by_program_and_collection.tsv"

gdc_entity_list=./auxiliary_metadata/__GDC_supplemental_metadata/GDC_entities_by_program_and_project.tsv

output_dir=./auxiliary_metadata/__aggregation_logs/projects

mkdir -p $output_dir

output_file="${output_dir}/naive_IDC_GDC_project_id_map.tsv"

echo ./package_root/auxiliary_scripts/420_draft_IDC_GDC_project_links_using_submitter_IDs.py $idc_entity_list $gdc_entity_list $output_file
./package_root/auxiliary_scripts/420_draft_IDC_GDC_project_links_using_submitter_IDs.py $idc_entity_list $gdc_entity_list $output_file

################################################################################
# ...then draft links from IDC projects to PDC projects based on entities of
# corresponding types sharing a submitter ID.
################################################################################

pdc_entity_list=./auxiliary_metadata/__PDC_supplemental_metadata/PDC_entities_by_program_project_and_study.tsv

output_file="${output_dir}/naive_IDC_PDC_project_id_map.tsv"

echo ./package_root/auxiliary_scripts/421_draft_IDC_PDC_project_links_using_submitter_IDs.py $idc_entity_list $pdc_entity_list $output_file
./package_root/auxiliary_scripts/421_draft_IDC_PDC_project_links_using_submitter_IDs.py $idc_entity_list $pdc_entity_list $output_file

################################################################################
# ...then draft links from IDC projects to CDS projects based on entities of
# corresponding types sharing a submitter ID.
################################################################################

cds_entity_list=./auxiliary_metadata/__CDS_supplemental_metadata/CDS_entities_by_program_and_study.tsv

output_file="${output_dir}/naive_IDC_CDS_project_id_map.tsv"

echo ./package_root/auxiliary_scripts/422_draft_IDC_CDS_project_links_using_submitter_IDs.py $idc_entity_list $cds_entity_list $output_file
./package_root/auxiliary_scripts/422_draft_IDC_CDS_project_links_using_submitter_IDs.py $idc_entity_list $cds_entity_list $output_file

################################################################################
# ...then draft links from IDC projects to ICDC projects based on entities of
# corresponding types sharing a submitter ID.
################################################################################

icdc_entity_list=./auxiliary_metadata/__ICDC_supplemental_metadata/ICDC_entities_by_program_and_study.tsv 

output_file="${output_dir}/naive_IDC_ICDC_project_id_map.tsv"

echo ./package_root/auxiliary_scripts/423_draft_IDC_ICDC_project_links_using_submitter_IDs.py $idc_entity_list $icdc_entity_list $output_file
./package_root/auxiliary_scripts/423_draft_IDC_ICDC_project_links_using_submitter_IDs.py $idc_entity_list $icdc_entity_list $output_file

>&2 echo

>&2 echo "Don't forget to hand-edit the project maps before proceeding!";

>&2 echo

>&2 echo


