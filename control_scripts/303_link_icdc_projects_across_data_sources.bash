#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*pl

# List ICDC entities and project affiliations (generate ${icdc_entity_list}, below) and enumerate the ICDC program-study hierarchy.

echo ./package_root/auxiliary_scripts/300_list_ICDC_entities_by_program_and_study_and_enumerate_ICDC_program_study_hierarchy.py
./package_root/auxiliary_scripts/300_list_ICDC_entities_by_program_and_study_and_enumerate_ICDC_program_study_hierarchy.py

# Draft links from ICDC projects to GDC projects based on entities of corresponding types sharing a submitter ID.

icdc_entity_list=./auxiliary_metadata/__ICDC_supplemental_metadata/ICDC_entities_by_program_and_study.tsv

gdc_entity_list=./auxiliary_metadata/__GDC_supplemental_metadata/GDC_entities_by_program_and_project.tsv

output_dir=./auxiliary_metadata/__aggregation_logs/projects

mkdir -p $output_dir

output_file="${output_dir}/naive_ICDC_GDC_project_id_map.tsv"

echo ./package_root/auxiliary_scripts/310_draft_ICDC_GDC_project_links_using_submitter_IDs.pl $icdc_entity_list $gdc_entity_list $output_file
./package_root/auxiliary_scripts/310_draft_ICDC_GDC_project_links_using_submitter_IDs.pl $icdc_entity_list $gdc_entity_list $output_file

# ...then draft links from ICDC projects to PDC projects based on entities of corresponding types sharing a submitter ID.

pdc_entity_list=./auxiliary_metadata/__PDC_supplemental_metadata/PDC_entities_by_program_project_and_study.tsv

output_file="${output_dir}/naive_ICDC_PDC_project_id_map.tsv"

echo ./package_root/auxiliary_scripts/311_draft_ICDC_PDC_project_links_using_submitter_IDs.pl $icdc_entity_list $pdc_entity_list $output_file
./package_root/auxiliary_scripts/311_draft_ICDC_PDC_project_links_using_submitter_IDs.pl $icdc_entity_list $pdc_entity_list $output_file

# ...then draft links from ICDC projects to CDS projects based on entities of corresponding types sharing a submitter ID.

cds_entity_list=./auxiliary_metadata/__CDS_supplemental_metadata/CDS_entities_by_program_and_study.tsv

output_file="${output_dir}/naive_ICDC_CDS_project_id_map.tsv"

echo ./package_root/auxiliary_scripts/312_draft_ICDC_CDS_project_links_using_submitter_IDs.pl $icdc_entity_list $cds_entity_list $output_file
./package_root/auxiliary_scripts/312_draft_ICDC_CDS_project_links_using_submitter_IDs.pl $icdc_entity_list $cds_entity_list $output_file

>&2 echo

>&2 echo "Don't forget to hand-edit the project maps before proceeding!";

>&2 echo

>&2 echo


