#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*py ./package_root/aggregate/phase_003_merge_icdc_into_gdc_and_pdc_and_cds/scripts/*.py

# Match ICDC subjects with GDC subjects based on submitter_id matches between cases in corresponding studies/projects.

echo ./package_root/auxiliary_scripts/320_match_ICDC_subjects_to_GDC_subjects.py 
./package_root/auxiliary_scripts/320_match_ICDC_subjects_to_GDC_subjects.py

echo ./package_root/auxiliary_scripts/321_match_ICDC_projects_to_GDC_projects.py
./package_root/auxiliary_scripts/321_match_ICDC_projects_to_GDC_projects.py

# Match ICDC subjects with PDC subjects based on submitter_id matches between cases in corresponding studies.

echo ./package_root/auxiliary_scripts/322_match_ICDC_subjects_to_PDC_subjects.py 
./package_root/auxiliary_scripts/322_match_ICDC_subjects_to_PDC_subjects.py

echo ./package_root/auxiliary_scripts/323_match_ICDC_projects_to_PDC_projects.py
./package_root/auxiliary_scripts/323_match_ICDC_projects_to_PDC_projects.py

# Match ICDC subjects with CDS subjects based on submitter_id matches between participants/cases in corresponding studies.

echo ./package_root/auxiliary_scripts/324_match_ICDC_subjects_to_CDS_subjects.py 
./package_root/auxiliary_scripts/324_match_ICDC_subjects_to_CDS_subjects.py

echo ./package_root/auxiliary_scripts/325_match_ICDC_projects_to_CDS_projects.py
./package_root/auxiliary_scripts/325_match_ICDC_projects_to_CDS_projects.py

# Cross-check merged GDC+PDC+CDS subjects to make sure independently-derived matches are sane, then build the final composed subject-merge map between ICDC and the already-merged GDC+PDC+CDS data.

echo ./package_root/auxiliary_scripts/326_match_ICDC_subjects_to_GDC_PDC_CDS_subjects.py
./package_root/auxiliary_scripts/326_match_ICDC_subjects_to_GDC_PDC_CDS_subjects.py

echo ./package_root/auxiliary_scripts/327_match_ICDC_projects_to_GDC_PDC_CDS_projects.py
./package_root/auxiliary_scripts/327_match_ICDC_projects_to_GDC_PDC_CDS_projects.py

# Merge ICDC into GDC+PDC_CDS.

echo ./package_root/aggregate/phase_003_merge_icdc_into_gdc_and_pdc_and_cds/scripts/merge_ICDC_CDA_data_into_GDC_PDC_CDS_CDA_data.py 
./package_root/aggregate/phase_003_merge_icdc_into_gdc_and_pdc_and_cds/scripts/merge_ICDC_CDA_data_into_GDC_PDC_CDS_CDA_data.py

# Link final data product with a rolling symlink to indicate the input dir for the next aggregation pass.

echo rm -f ./cda_tsvs/last_merge
rm -f ./cda_tsvs/last_merge

echo ln -s merged_gdc_pdc_cds_and_icdc_002_decorated_harmonized ./cda_tsvs/last_merge
ln -s merged_gdc_pdc_cds_and_icdc_002_decorated_harmonized ./cda_tsvs/last_merge


