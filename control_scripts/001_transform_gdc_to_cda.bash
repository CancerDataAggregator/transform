#!/usr/bin/env bash

ln -sf venv/lib/python*/site-packages/cda_etl package_root

chmod 755 ./package_root/transform/gdc/scripts/*py ./package_root/auxiliary_scripts/*py

echo ./package_root/transform/gdc/scripts/transform_gdc_files_to_CDA_TSV.py
./package_root/transform/gdc/scripts/transform_gdc_files_to_CDA_TSV.py

echo ./package_root/transform/gdc/scripts/transform_gdc_subjects_to_CDA_TSV.py
./package_root/transform/gdc/scripts/transform_gdc_subjects_to_CDA_TSV.py

echo ./package_root/auxiliary_scripts/999_harmonize_cda_tsvs.py ./cda_tsvs/gdc_raw_unharmonized ./cda_tsvs/gdc ./auxiliary_metadata/__substitution_logs/gdc
./package_root/auxiliary_scripts/999_harmonize_cda_tsvs.py ./cda_tsvs/gdc_raw_unharmonized ./cda_tsvs/gdc ./auxiliary_metadata/__substitution_logs/gdc


