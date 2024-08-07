#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*pl

gdc_file=./auxiliary_metadata/__GDC_supplemental_metadata/GDC_entity_submitter_id_to_program_name_and_project_id.tsv

pdc_file=./auxiliary_metadata/__PDC_supplemental_metadata/PDC_entity_submitter_id_to_program_project_and_study.tsv

idc_file=./auxiliary_metadata/__IDC_supplemental_metadata/IDC_entity_submitter_id_to_collection_id.tsv

worker_script=./package_root/auxiliary_scripts/300_naively_equate_PDC_project_and_study_submitter_IDs_to_IDC_collection_IDs.pl

output_file=./auxiliary_metadata/__project_crossrefs/naive_IDC-PDC_project_id_map.tsv

mkdir -p ./auxiliary_metadata/__project_crossrefs

echo $worker_script $pdc_file $idc_file $output_file
$worker_script $pdc_file $idc_file $output_file

worker_script=./package_root/auxiliary_scripts/301_naively_equate_IDC_collection_IDs_to_GDC_project_IDs.pl

output_file=./auxiliary_metadata/__project_crossrefs/naive_GDC-IDC_project_id_map.tsv

echo $worker_script $idc_file $gdc_file $output_file
$worker_script $idc_file $gdc_file $output_file

>&2 echo

>&2 echo "Don't forget to hand-edit the project map before proceeding!";

>&2 echo

