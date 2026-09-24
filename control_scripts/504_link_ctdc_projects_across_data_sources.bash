#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*p[yl]

################################################################################
# List CTDC entities and Study affiliations (generate ${ctdc_entity_list},
# below) and enumerate the CTDC Study list.
################################################################################
ctdc_study_source_file=extracted_data/ctdc_postprocessed/Study/Study.tsv
ctdc_aux_supplemental_data_dir=./auxiliary_metadata/__CTDC_supplemental_metadata
mkdir -p $ctdc_aux_supplemental_data_dir
ctdc_study_target_file="${ctdc_aux_supplemental_data_dir}/CTDC_all_Studies.tsv"
ln -sf ../../$ctdc_study_source_file $ctdc_study_target_file
echo ./package_root/auxiliary_scripts/500_list_CTDC_entities_by_Study.py
./package_root/auxiliary_scripts/500_list_CTDC_entities_by_Study.py

output_dir=./auxiliary_metadata/__aggregation_logs/projects
mkdir -p $output_dir

################################################################################
# Draft links from CTDC Studies to GDC projects based on entities of
# corresponding types sharing a submitter ID.
################################################################################
ctdc_entity_list="${ctdc_aux_supplemental_data_dir}/CTDC_entities_by_Study.tsv"
gdc_entity_list=./auxiliary_metadata/__GDC_supplemental_metadata/GDC_entities_by_program_and_project.tsv
output_file="${output_dir}/naive_CTDC_GDC_project_id_map.tsv"
echo ./package_root/auxiliary_scripts/510_draft_CTDC_GDC_project_links_using_submitter_IDs.py $ctdc_entity_list $gdc_entity_list $output_file
./package_root/auxiliary_scripts/510_draft_CTDC_GDC_project_links_using_submitter_IDs.py $ctdc_entity_list $gdc_entity_list $output_file

################################################################################
# ...then draft links from CTDC Studies to PDC projects based on entities of
# corresponding types sharing a submitter ID.
################################################################################
pdc_entity_list=./auxiliary_metadata/__PDC_supplemental_metadata/PDC_entities_by_program_project_and_study.tsv
output_file="${output_dir}/naive_CTDC_PDC_project_id_map.tsv"
echo ./package_root/auxiliary_scripts/511_draft_CTDC_PDC_project_links_using_submitter_IDs.py $ctdc_entity_list $pdc_entity_list $output_file
./package_root/auxiliary_scripts/511_draft_CTDC_PDC_project_links_using_submitter_IDs.py $ctdc_entity_list $pdc_entity_list $output_file

################################################################################
# ...then draft links from CTDC Studies to GC projects based on entities of
# corresponding types sharing a submitter ID.
################################################################################
gc_entity_list=./auxiliary_metadata/__GC_supplemental_metadata/GC_entities_by_program_and_study.tsv
output_file="${output_dir}/naive_CTDC_GC_project_id_map.tsv"
echo ./package_root/auxiliary_scripts/512_draft_CTDC_GC_project_links_using_submitter_IDs.py $ctdc_entity_list $gc_entity_list $output_file
./package_root/auxiliary_scripts/512_draft_CTDC_GC_project_links_using_submitter_IDs.py $ctdc_entity_list $gc_entity_list $output_file

################################################################################
# ...then draft links from CTDC projects to ICDC projects based on entities of
# corresponding types sharing a submitter ID.
################################################################################
icdc_entity_list=./auxiliary_metadata/__ICDC_supplemental_metadata/ICDC_entities_by_program_and_study.tsv 
output_file="${output_dir}/naive_CTDC_ICDC_project_id_map.tsv"
echo ./package_root/auxiliary_scripts/513_draft_CTDC_ICDC_project_links_using_submitter_IDs.py $ctdc_entity_list $icdc_entity_list $output_file
./package_root/auxiliary_scripts/513_draft_CTDC_ICDC_project_links_using_submitter_IDs.py $ctdc_entity_list $icdc_entity_list $output_file

################################################################################
# ...then draft links from CTDC projects to IDC collections based on entities of
# corresponding types sharing a submitter ID.
################################################################################
idc_entity_list=./auxiliary_metadata/__IDC_supplemental_metadata/IDC_entities_by_program_and_collection.tsv
output_file="${output_dir}/naive_CTDC_IDC_project_id_map.tsv"
echo ./package_root/auxiliary_scripts/514_draft_CTDC_IDC_project_links_using_submitter_IDs.py $ctdc_entity_list $idc_entity_list $output_file
./package_root/auxiliary_scripts/514_draft_CTDC_IDC_project_links_using_submitter_IDs.py $ctdc_entity_list $idc_entity_list $output_file

>&2 echo
>&2 echo "Don't forget to hand-edit the project maps before proceeding!";
>&2 echo
>&2 echo


