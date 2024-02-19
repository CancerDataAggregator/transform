#!/usr/bin/env bash

chmod 755 ./package_root/extract/cds/scripts/*py

echo ./package_root/extract/cds/scripts/000_get_schema_via_introspection.py
./package_root/extract/cds/scripts/000_get_schema_via_introspection.py

echo ./package_root/extract/cds/scripts/010_get_cds_release_summary_metadata.py
./package_root/extract/cds/scripts/010_get_cds_release_summary_metadata.py

echo ./package_root/extract/cds/scripts/011_get_program_and_study_metadata.py
./package_root/extract/cds/scripts/011_get_program_and_study_metadata.py

echo ./package_root/extract/cds/scripts/012_get_participant_metadata.py
./package_root/extract/cds/scripts/012_get_participant_metadata.py

echo ./package_root/extract/cds/scripts/013_get_sample_and_specimen_metadata.py
./package_root/extract/cds/scripts/013_get_sample_and_specimen_metadata.py

echo ./package_root/extract/cds/scripts/014_get_diagnosis_metadata.py
./package_root/extract/cds/scripts/014_get_diagnosis_metadata.py

echo ./package_root/extract/cds/scripts/015_get_treatment_metadata.py
./package_root/extract/cds/scripts/015_get_treatment_metadata.py

echo ./package_root/extract/cds/scripts/016_get_file_metadata.py
./package_root/extract/cds/scripts/016_get_file_metadata.py

echo ./package_root/extract/cds/scripts/017_get_genomic_info_metadata.RECORDS_NOT_UNIQUE.py
./package_root/extract/cds/scripts/017_get_genomic_info_metadata.RECORDS_NOT_UNIQUE.py


