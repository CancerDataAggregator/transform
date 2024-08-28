#!/usr/bin/env bash

chmod 755 package_root/extract/icdc/scripts/*py

echo ./package_root/extract/icdc/scripts/000_get_schema_via_introspection.py
./package_root/extract/icdc/scripts/000_get_schema_via_introspection.py

echo ./package_root/extract/icdc/scripts/010_get_case_metadata_and_log_extraction_date.py
./package_root/extract/icdc/scripts/010_get_case_metadata_and_log_extraction_date.py

echo ./package_root/extract/icdc/scripts/011_get_file_metadata.py
./package_root/extract/icdc/scripts/011_get_file_metadata.py

echo ./package_root/extract/icdc/scripts/012_get_diagnosis_metadata.py
./package_root/extract/icdc/scripts/012_get_diagnosis_metadata.py

echo ./package_root/extract/icdc/scripts/013_get_sample_metadata.py
./package_root/extract/icdc/scripts/013_get_sample_metadata.py

echo ./package_root/extract/icdc/scripts/014_get_visit_metadata.py
./package_root/extract/icdc/scripts/014_get_visit_metadata.py

echo ./package_root/extract/icdc/scripts/015_get_enrollment_metadata.py
./package_root/extract/icdc/scripts/015_get_enrollment_metadata.py

echo ./package_root/extract/icdc/scripts/016_get_adverse_event_metadata.py
./package_root/extract/icdc/scripts/016_get_adverse_event_metadata.py

echo ./package_root/extract/icdc/scripts/017_get_agent_metadata.py
./package_root/extract/icdc/scripts/017_get_agent_metadata.py

echo ./package_root/extract/icdc/scripts/018_get_agent_administration_metadata.py
./package_root/extract/icdc/scripts/018_get_agent_administration_metadata.py

echo ./package_root/extract/icdc/scripts/019_get_cycle_metadata.py
./package_root/extract/icdc/scripts/019_get_cycle_metadata.py

echo ./package_root/extract/icdc/scripts/020_get_study_arm_metadata.py
./package_root/extract/icdc/scripts/020_get_study_arm_metadata.py

echo ./package_root/extract/icdc/scripts/021_get_cohort_metadata.py
./package_root/extract/icdc/scripts/021_get_cohort_metadata.py

echo ./package_root/extract/icdc/scripts/022_get_study_metadata.py
./package_root/extract/icdc/scripts/022_get_study_metadata.py

echo ./package_root/extract/icdc/scripts/023_get_program_metadata.py
./package_root/extract/icdc/scripts/023_get_program_metadata.py

echo ./package_root/extract/icdc/scripts/024_get_principal_investigator_metadata.py
./package_root/extract/icdc/scripts/024_get_principal_investigator_metadata.py

echo ./package_root/extract/icdc/scripts/025_get_demographic_metadata.py
./package_root/extract/icdc/scripts/025_get_demographic_metadata.py

echo ./package_root/extract/icdc/scripts/026_get_biospecimen_source_metadata.py
./package_root/extract/icdc/scripts/026_get_biospecimen_source_metadata.py


