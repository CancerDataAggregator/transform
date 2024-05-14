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

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/009_Exposure.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/009_Exposure.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/010_FollowUp.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/010_FollowUp.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/011_FamilyHistory.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/011_FamilyHistory.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/012_Treatment.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/012_Treatment.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/020_Case.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/020_Case.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/021_link_Demographic_Diagnosis_Exposure_FollowUp_FamilyHistory_and_Treatment_to_study_id.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/021_link_Demographic_Diagnosis_Exposure_FollowUp_FamilyHistory_and_Treatment_to_study_id.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/030_Protocol.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/030_Protocol.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/031_WorkflowMetadata.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/031_WorkflowMetadata.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/040_StudyRunMetadata.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/040_StudyRunMetadata.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/041_AliquotRunMetadata.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/041_AliquotRunMetadata.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/090_Reference.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/090_Reference.py

echo ./package_root/transform/pdc/scripts/phase_001_collate_and_filter/099_filter_intrastudy_submitter_id_duplicates_and_studyless_cases.py
./package_root/transform/pdc/scripts/phase_001_collate_and_filter/099_filter_intrastudy_submitter_id_duplicates_and_studyless_cases.py


