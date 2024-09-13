#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*py ./package_root/aggregate/phase_004_merge_idc_into_gdc_and_pdc_and_cds_and_icdc/scripts/*.py

# Match IDC subjects with GDC subjects based on submitter_id matches between cases in corresponding collections/projects.

echo ./package_root/auxiliary_scripts/430_match_IDC_subjects_to_GDC_subjects.py 
./package_root/auxiliary_scripts/430_match_IDC_subjects_to_GDC_subjects.py 

echo ./package_root/auxiliary_scripts/431_match_IDC_projects_to_GDC_projects.py
./package_root/auxiliary_scripts/431_match_IDC_projects_to_GDC_projects.py

# Match IDC subjects with PDC subjects based on submitter_id matches between cases in corresponding collections/studies.

echo ./package_root/auxiliary_scripts/432_match_IDC_subjects_to_PDC_subjects.py 
./package_root/auxiliary_scripts/432_match_IDC_subjects_to_PDC_subjects.py 

echo ./package_root/auxiliary_scripts/433_match_IDC_projects_to_PDC_projects.py
./package_root/auxiliary_scripts/433_match_IDC_projects_to_PDC_projects.py

# Match IDC subjects with CDS subjects based on submitter_id matches between cases/participants in corresponding collections/studies.

echo ./package_root/auxiliary_scripts/434_match_IDC_subjects_to_CDS_subjects.py 
./package_root/auxiliary_scripts/434_match_IDC_subjects_to_CDS_subjects.py 

echo ./package_root/auxiliary_scripts/435_match_IDC_projects_to_CDS_projects.py
./package_root/auxiliary_scripts/435_match_IDC_projects_to_CDS_projects.py

# Match IDC subjects with ICDC subjects based on submitter_id matches between cases in corresponding collections/studies.

echo ./package_root/auxiliary_scripts/436_match_IDC_subjects_to_ICDC_subjects.py 
./package_root/auxiliary_scripts/436_match_IDC_subjects_to_ICDC_subjects.py 

echo ./package_root/auxiliary_scripts/437_match_IDC_projects_to_ICDC_projects.py
./package_root/auxiliary_scripts/437_match_IDC_projects_to_ICDC_projects.py

# Cross-check merged GDC+PDC+CDS+ICDC subjects to make sure independently-derived matches are sane, then build the final composed subject-merge map between IDC and the already-merged GDC+PDC+CDS+ICDC data.

echo ./package_root/auxiliary_scripts/438_match_IDC_subjects_to_GDC_PDC_CDS_ICDC_subjects.py
./package_root/auxiliary_scripts/438_match_IDC_subjects_to_GDC_PDC_CDS_ICDC_subjects.py

echo ./package_root/auxiliary_scripts/439_match_IDC_projects_to_GDC_PDC_CDS_ICDC_projects.py
./package_root/auxiliary_scripts/439_match_IDC_projects_to_GDC_PDC_CDS_ICDC_projects.py

# Merge IDC into GDC+PDC+CDS+ICDC.

echo ./package_root/aggregate/phase_004_merge_idc_into_gdc_and_pdc_and_cds_and_icdc/scripts/merge_IDC_CDA_data_into_GDC_PDC_CDS_ICDC_CDA_data.py
./package_root/aggregate/phase_004_merge_idc_into_gdc_and_pdc_and_cds_and_icdc/scripts/merge_IDC_CDA_data_into_GDC_PDC_CDS_ICDC_CDA_data.py

# Link final data product with a rolling symlink to indicate the input dir for the next aggregation pass.

echo rm -f ./cda_tsvs/last_merge
rm -f ./cda_tsvs/last_merge

echo ln -s merged_gdc_pdc_cds_icdc_and_idc_002_decorated_harmonized ./cda_tsvs/last_merge
ln -s merged_gdc_pdc_cds_icdc_and_idc_002_decorated_harmonized ./cda_tsvs/last_merge


