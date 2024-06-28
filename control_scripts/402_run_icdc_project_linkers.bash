#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*pl

icdc_file=./auxiliary_metadata/__ICDC_supplemental_metadata/ICDC_entity_id_to_program_and_study.tsv

cds_file=./auxiliary_metadata/__CDS_supplemental_metadata/CDS_entity_submitter_id_to_program_and_study.tsv

gdc_file=./auxiliary_metadata/__project_crossrefs/GDC_entity_submitter_id_to_program_name_and_project_id.tsv

pdc_file=./auxiliary_metadata/__project_crossrefs/PDC_entity_submitter_id_to_program_project_and_study.tsv

idc_file=./auxiliary_metadata/__IDC_supplemental_metadata/IDC_entity_submitter_id_to_collection_id.tsv

# ...

worker_script=./package_root/auxiliary_scripts/500_naively_equate_ICDC_program_and_study_IDs_to_GDC_program_and_project_IDs.pl

output_file=./auxiliary_metadata/__project_crossrefs/naive_ICDC-GDC_project_id_map.tsv

echo $worker_script $icdc_file $gdc_file $output_file
$worker_script $icdc_file $gdc_file $output_file

# ...

worker_script=./package_root/auxiliary_scripts/501_naively_equate_ICDC_program_and_study_IDs_to_PDC_program_project__and_study_IDs.pl

output_file=./auxiliary_metadata/__project_crossrefs/naive_ICDC-PDC_project_id_map.tsv

echo $worker_script $icdc_file $pdc_file $output_file
$worker_script $icdc_file $pdc_file $output_file

# ...

worker_script=./package_root/auxiliary_scripts/502_naively_equate_ICDC_program_and_study_IDs_to_IDC_collection_IDs.pl

output_file=./auxiliary_metadata/__project_crossrefs/naive_ICDC-IDC_project_id_map.tsv

echo $worker_script $icdc_file $idc_file $output_file
$worker_script $icdc_file $idc_file $output_file

# ...

worker_script=./package_root/auxiliary_scripts/503_naively_equate_ICDC_program_and_study_IDs_to_CDS_program_and_study_IDs.pl

output_file=./auxiliary_metadata/__project_crossrefs/naive_ICDC-CDS_project_id_map.tsv

echo $worker_script $icdc_file $cds_file $output_file
$worker_script $icdc_file $cds_file $output_file

# ...

>&2 echo

>&2 echo "Don't forget to hand-edit the project map before proceeding!";

>&2 echo

>&2 echo

