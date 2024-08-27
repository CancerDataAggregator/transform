#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*py ./package_root/aggregate/phase_002_merge_cds_into_gdc_and_pdc/scripts/*.py

# Match CDS subjects with GDC subjects based on submitter_id matches between particpants/cases in corresponding studies/projects.

echo ./package_root/auxiliary_scripts/213_match_CDS_subjects_to_GDC_subjects.py 
./package_root/auxiliary_scripts/213_match_CDS_subjects_to_GDC_subjects.py

echo ./package_root/auxiliary_scripts/214_match_CDS_projects_to_GDC_projects.py
./package_root/auxiliary_scripts/214_match_CDS_projects_to_GDC_projects.py

# Match CDS subjects with PDC subjects based on submitter_id matches between participants/cases in corresponding studies.

echo ./package_root/auxiliary_scripts/215_match_CDS_subjects_to_PDC_subjects.py 
./package_root/auxiliary_scripts/215_match_CDS_subjects_to_PDC_subjects.py

echo ./package_root/auxiliary_scripts/216_match_CDS_projects_to_PDC_projects.py
./package_root/auxiliary_scripts/216_match_CDS_projects_to_PDC_projects.py

# Cross-check merged GDC+PDC subjects to make sure independently-derived matches are sane, then build the final composed subject-merge map between CDS and the already-merged GDC+PDC data.

echo ./package_root/auxiliary_scripts/217_match_CDS_subjects_to_GDC_PDC_subjects.py
./package_root/auxiliary_scripts/217_match_CDS_subjects_to_GDC_PDC_subjects.py

echo ./package_root/auxiliary_scripts/218_match_CDS_projects_to_GDC_PDC_projects.py
./package_root/auxiliary_scripts/218_match_CDS_projects_to_GDC_PDC_projects.py

echo ./package_root/aggregate/phase_002_merge_cds_into_gdc_and_pdc/scripts/merge_CDS_CDA_data_into_PDC_GDC_CDA_data.py
./package_root/aggregate/phase_002_merge_cds_into_gdc_and_pdc/scripts/merge_CDS_CDA_data_into_PDC_GDC_CDA_data.py

# Link final data product with a rolling symlink to indicate the input dir for the next aggregation pass.

echo rm -f ./cda_tsvs/last_merge
rm -f ./cda_tsvs/last_merge

echo ln -s merged_gdc_and_pdc_002_decorated_harmonized ./cda_tsvs/last_merge
ln -s merged_gdc_pdc_and_cds_002_decorated_harmonized ./cda_tsvs/last_merge


