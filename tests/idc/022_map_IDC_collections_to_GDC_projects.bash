#!/usr/bin/env bash

worker_script=tests/helpers/002_naively_equate_IDC_collection_IDs_to_GDC_project_IDs.pl 

gdc_file=extracted_data/idc/v13/__supplemental_metadata/GDC_entity_submitter_id_to_program_name_and_project_id.tsv

pdc_file=extracted_data/idc/v13/__supplemental_metadata/PDC_entity_submitter_id_to_program_project_and_study.tsv

idc_file=extracted_data/idc/v13/__supplemental_metadata/IDC_entity_submitter_id_to_collection_id.tsv

output_file=extracted_data/idc/v13/__supplemental_metadata/naive_GDC-IDC_project_id_map.tsv

./$worker_script $idc_file $gdc_file $output_file


