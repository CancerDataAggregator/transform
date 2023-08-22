#!/usr/bin/env bash

rm -f extracted_data/pdc_postprocessed/__filtration_logs/*

chmod 755 ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/*py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/001_File.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/001_File.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/002_Program.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/002_Program.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/003_Project.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/003_Project.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/004_Study.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/004_Study.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/005_Sample.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/005_Sample.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/006_Aliquot.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/006_Aliquot.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/007_Demographic.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/007_Demographic.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/008_Diagnosis.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/008_Diagnosis.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/009_Case.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/009_Case.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/010_Reference.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/010_Reference.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/011_link_Demographic_and_Diagnosis_to_study_id.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/011_link_Demographic_and_Diagnosis_to_study_id.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/012_filter_intrastudy_submitter_id_duplicates_and_studyless_cases.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/012_filter_intrastudy_submitter_id_duplicates_and_studyless_cases.py


