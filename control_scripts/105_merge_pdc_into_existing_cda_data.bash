#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*py ./package_root/aggregate/phase_001_merge_pdc_into_gdc/scripts/*py

echo ./package_root/auxiliary_scripts/120_match_PDC_subjects_to_GDC_subjects.py
./package_root/auxiliary_scripts/120_match_PDC_subjects_to_GDC_subjects.py

echo ./package_root/auxiliary_scripts/121_match_PDC_projects_to_GDC_projects.py
./package_root/auxiliary_scripts/121_match_PDC_projects_to_GDC_projects.py

echo ./package_root/aggregate/phase_001_merge_pdc_into_gdc/scripts/merge_PDC_CDA_data_into_GDC_CDA_data.py
./package_root/aggregate/phase_001_merge_pdc_into_gdc/scripts/merge_PDC_CDA_data_into_GDC_CDA_data.py

# Link final data product with a rolling symlink to indicate the input dir for the next aggregation pass.

echo rm -f ./cda_tsvs/last_merge
rm -f ./cda_tsvs/last_merge

echo ln -s merged_gdc_and_pdc_002_decorated_harmonized ./cda_tsvs/last_merge
ln -s merged_gdc_and_pdc_002_decorated_harmonized ./cda_tsvs/last_merge


