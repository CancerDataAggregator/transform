#!/usr/bin/env bash

chmod 755 ./199_idc_controllers/*py ./package_root/auxiliary_scripts/*py

echo ./199_idc_controllers/010_extract_submitter_IDs_from_auxiliary_metadata_to_tsv.py
./199_idc_controllers/010_extract_submitter_IDs_from_auxiliary_metadata_to_tsv.py

echo ./199_idc_controllers/011_extract_file_subject_and_rs_metadata_to_tsv.py
./199_idc_controllers/011_extract_file_subject_and_rs_metadata_to_tsv.py

echo ./199_idc_controllers/012_map_case_ids_to_collection_ids.py
./199_idc_controllers/012_map_case_ids_to_collection_ids.py

echo ./199_idc_controllers/014_populate_rs_primary_diagnosis_condition.py
./199_idc_controllers/014_populate_rs_primary_diagnosis_condition.py

echo ./199_idc_controllers/015_populate_tcga_clinical_subject_metadata.py
./199_idc_controllers/015_populate_tcga_clinical_subject_metadata.py

echo ./199_idc_controllers/016_populate_tcga_biospecimen_metadata.py
./199_idc_controllers/016_populate_tcga_biospecimen_metadata.py

echo ./package_root/auxiliary_scripts/999_harmonize_cda_tsvs.py ./cda_tsvs/idc_raw_unharmonized/v17 ./cda_tsvs/idc/v17 ./auxiliary_metadata/__substitution_logs/idc
./package_root/auxiliary_scripts/999_harmonize_cda_tsvs.py ./cda_tsvs/idc_raw_unharmonized/v17 ./cda_tsvs/idc/v17 ./auxiliary_metadata/__substitution_logs/idc


