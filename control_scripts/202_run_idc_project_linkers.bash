#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*pl

worker_script=./package_root/auxiliary_scripts/500_naively_equate_PDC_project_and_study_submitter_IDs_to_IDC_collection_IDs.pl

gdc_file=./auxiliary_metadata/__project_crossrefs/GDC_entity_submitter_id_to_program_name_and_project_id.tsv

pdc_file=./auxiliary_metadata/__project_crossrefs/PDC_entity_submitter_id_to_program_project_and_study.tsv

idc_file=./auxiliary_metadata/__IDC_supplemental_metadata/IDC_entity_submitter_id_to_collection_id.tsv

output_file=./auxiliary_metadata/__project_crossrefs/naive_IDC-PDC_project_id_map.tsv

$worker_script $pdc_file $idc_file $output_file

worker_script=./package_root/auxiliary_scripts/501_naively_equate_IDC_collection_IDs_to_GDC_project_IDs.pl

output_file=./auxiliary_metadata/__project_crossrefs/naive_GDC-IDC_project_id_map.tsv

$worker_script $idc_file $gdc_file $output_file


