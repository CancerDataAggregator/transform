#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*py ./package_root/aggregate/phase_005_merge_ctdc_into_gdc_and_pdc_and_gc_and_icdc_and_idc/scripts/*.py

# Match CTDC subjects with GDC subjects based on submitter_id matches between cases in corresponding studies/projects.
echo ./package_root/auxiliary_scripts/520_match_CTDC_subjects_to_GDC_subjects.py 
./package_root/auxiliary_scripts/520_match_CTDC_subjects_to_GDC_subjects.py 
echo ./package_root/auxiliary_scripts/521_match_CTDC_projects_to_GDC_projects.py
./package_root/auxiliary_scripts/521_match_CTDC_projects_to_GDC_projects.py

# Match CTDC subjects with PDC subjects based on submitter_id matches between cases in corresponding studies.
echo ./package_root/auxiliary_scripts/522_match_CTDC_subjects_to_PDC_subjects.py
./package_root/auxiliary_scripts/522_match_CTDC_subjects_to_PDC_subjects.py
echo ./package_root/auxiliary_scripts/523_match_CTDC_projects_to_PDC_projects.py
./package_root/auxiliary_scripts/523_match_CTDC_projects_to_PDC_projects.py

# Match CTDC subjects with GC subjects based on submitter_id matches between cases/participants in corresponding studies.
echo ./package_root/auxiliary_scripts/524_match_CTDC_subjects_to_GC_subjects.py 
./package_root/auxiliary_scripts/524_match_CTDC_subjects_to_GC_subjects.py 
echo ./package_root/auxiliary_scripts/525_match_CTDC_projects_to_GC_projects.py
./package_root/auxiliary_scripts/525_match_CTDC_projects_to_GC_projects.py

# Match CTDC subjects with ICDC subjects based on submitter_id matches between cases in corresponding studies.
echo ./package_root/auxiliary_scripts/526_match_CTDC_subjects_to_ICDC_subjects.py 
./package_root/auxiliary_scripts/526_match_CTDC_subjects_to_ICDC_subjects.py 
echo ./package_root/auxiliary_scripts/527_match_CTDC_projects_to_ICDC_projects.py
./package_root/auxiliary_scripts/527_match_CTDC_projects_to_ICDC_projects.py

# Match CTDC subjects with IDC subjects based on submitter_id matches between cases in corresponding collections/studies.
echo ./package_root/auxiliary_scripts/528_match_CTDC_subjects_to_IDC_subjects.py
./package_root/auxiliary_scripts/528_match_CTDC_subjects_to_IDC_subjects.py
echo ./package_root/auxiliary_scripts/529_match_CTDC_projects_to_IDC_projects.py
./package_root/auxiliary_scripts/529_match_CTDC_projects_to_IDC_projects.py

# Cross-check merged GDC+PDC+GC+ICDC+IDC subjects to make sure independently-derived matches are sane, then build the final composed subject-merge map between CTDC and the already-merged GDC+PDC+GC+ICDC+IDC data.
echo ./package_root/auxiliary_scripts/530_match_CTDC_subjects_to_GDC_PDC_GC_ICDC_IDC_subjects.py
./package_root/auxiliary_scripts/530_match_CTDC_subjects_to_GDC_PDC_GC_ICDC_IDC_subjects.py
echo ./package_root/auxiliary_scripts/531_match_CTDC_projects_to_GDC_PDC_GC_ICDC_IDC_projects.py
./package_root/auxiliary_scripts/531_match_CTDC_projects_to_GDC_PDC_GC_ICDC_IDC_projects.py

# Merge CTDC into GDC+PDC+GC+ICDC+IDC.
echo ./package_root/aggregate/phase_005_merge_ctdc_into_gdc_and_pdc_and_gc_and_icdc_and_idc/scripts/merge_CTDC_CDA_data_into_GDC_PDC_GC_ICDC_IDC_CDA_data.py
./package_root/aggregate/phase_005_merge_ctdc_into_gdc_and_pdc_and_gc_and_icdc_and_idc/scripts/merge_CTDC_CDA_data_into_GDC_PDC_GC_ICDC_IDC_CDA_data.py

# Link final data product with a rolling symlink to indicate the input dir for the next aggregation pass.
echo rm -f ./cda_tsvs/last_merge
rm -f ./cda_tsvs/last_merge
echo ln -s merged_gdc_pdc_gc_icdc_idc_and_ctdc_002_decorated_harmonized ./cda_tsvs/last_merge
ln -s merged_gdc_pdc_gc_icdc_idc_and_ctdc_002_decorated_harmonized ./cda_tsvs/last_merge


