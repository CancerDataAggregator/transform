#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*pl

cds_file=./auxiliary_metadata/__CDS_supplemental_metadata/CDS_entity_submitter_id_to_program_and_study.tsv

gdc_file=./auxiliary_metadata/__GDC_supplemental_metadata/GDC_entity_submitter_id_to_program_name_and_project_id.tsv

pdc_file=./auxiliary_metadata/__PDC_supplemental_metadata/PDC_entity_submitter_id_to_program_project_and_study.tsv

idc_file=./auxiliary_metadata/__IDC_supplemental_metadata/IDC_entity_submitter_id_to_collection_id.tsv

# ...

worker_script=./package_root/auxiliary_scripts/400_naively_equate_CDS_program_and_study_IDs_to_GDC_program_and_project_IDs.pl

output_file=./auxiliary_metadata/__project_crossrefs/naive_CDS-GDC_project_id_map.tsv

echo $worker_script $cds_file $gdc_file $output_file
$worker_script $cds_file $gdc_file $output_file

# ...

worker_script=./package_root/auxiliary_scripts/401_naively_equate_CDS_program_and_study_IDs_to_PDC_program_project_and_study_IDs.pl

output_file=./auxiliary_metadata/__project_crossrefs/naive_CDS-PDC_project_id_map.tsv

echo $worker_script $cds_file $pdc_file $output_file
$worker_script $cds_file $pdc_file $output_file

# ...

worker_script=./package_root/auxiliary_scripts/402_naively_equate_CDS_program_and_study_IDs_to_IDC_collection_IDs.pl

output_file=./auxiliary_metadata/__project_crossrefs/naive_CDS-IDC_project_id_map.tsv

echo $worker_script $cds_file $idc_file $output_file
$worker_script $cds_file $idc_file $output_file

# ...

>&2 echo

>&2 echo "Don't forget to hand-edit the project map before proceeding!";

>&2 echo

>&2 echo

