#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*p[yl]

# List CDS entities and project affiliations (generate ${cds_entity_list}, below) and enumerate the CDS program-study hierarchy.

echo ./package_root/auxiliary_scripts/210_list_CDS_entities_by_program_and_study_and_enumerate_CDS_program_study_hierarchy.py
./package_root/auxiliary_scripts/210_list_CDS_entities_by_program_and_study_and_enumerate_CDS_program_study_hierarchy.py

# Draft links from CDS projects to GDC projects based on entities of corresponding types sharing a submitter ID.

cds_entity_list=./auxiliary_metadata/__CDS_supplemental_metadata/CDS_entities_by_program_and_study.tsv

gdc_entity_list=./auxiliary_metadata/__GDC_supplemental_metadata/GDC_entities_by_program_and_project.tsv

output_dir=./auxiliary_metadata/__aggregation_logs/projects

mkdir -p $output_dir

output_file="${output_dir}/naive_CDS_GDC_project_id_map.tsv"

echo ./package_root/auxiliary_scripts/220_draft_CDS_GDC_project_links_using_submitter_IDs.pl $cds_entity_list $gdc_entity_list $output_file
./package_root/auxiliary_scripts/220_draft_CDS_GDC_project_links_using_submitter_IDs.pl $cds_entity_list $gdc_entity_list $output_file

# ...then draft links from CDS projects to PDC projects based on entities of corresponding types sharing a submitter ID.

pdc_entity_list=./auxiliary_metadata/__PDC_supplemental_metadata/PDC_entities_by_program_project_and_study.tsv

output_file="${output_dir}/naive_CDS_PDC_project_id_map.tsv"

echo ./package_root/auxiliary_scripts/221_draft_CDS_PDC_project_links_using_submitter_IDs.pl $cds_entity_list $pdc_entity_list $output_file
./package_root/auxiliary_scripts/221_draft_CDS_PDC_project_links_using_submitter_IDs.pl $cds_entity_list $pdc_entity_list $output_file

>&2 echo

>&2 echo "Don't forget to hand-edit the project maps before proceeding!";

>&2 echo

>&2 echo


