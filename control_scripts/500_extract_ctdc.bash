#!/usr/bin/env bash

chmod 755 package_root/extract/ctdc/scripts/*py

echo ./package_root/extract/ctdc/scripts/000_get_CTDC_schema_via_introspection.py
./package_root/extract/ctdc/scripts/000_get_CTDC_schema_via_introspection.py

echo ./package_root/extract/ctdc/scripts/010_getAllStudies_and_log_extraction_date_and_version_as_fetch_date.py
./package_root/extract/ctdc/scripts/010_getAllStudies_and_log_extraction_date_and_version_as_fetch_date.py

echo ./package_root/extract/ctdc/scripts/011_studyDiagnosisByStudyShortName.py
./package_root/extract/ctdc/scripts/011_studyDiagnosisByStudyShortName.py

echo ./package_root/extract/ctdc/scripts/012_StudySpecimenByStudyShortName.py
./package_root/extract/ctdc/scripts/012_StudySpecimenByStudyShortName.py

echo ./package_root/extract/ctdc/scripts/013_StudyDataFileByStudyShortName.py
./package_root/extract/ctdc/scripts/013_StudyDataFileByStudyShortName.py

echo ./package_root/extract/ctdc/scripts/014_studyZipFileQuery.py
./package_root/extract/ctdc/scripts/014_studyZipFileQuery.py

echo ./package_root/extract/ctdc/scripts/020_participantOverview.py
./package_root/extract/ctdc/scripts/020_participantOverview.py

echo ./package_root/extract/ctdc/scripts/021_participant_data_files.py
./package_root/extract/ctdc/scripts/021_participant_data_files.py

echo ./package_root/extract/ctdc/scripts/030_biospecimenOverview.py
./package_root/extract/ctdc/scripts/030_biospecimenOverview.py

echo ./package_root/extract/ctdc/scripts/031_biospecimen_data_files.py
./package_root/extract/ctdc/scripts/031_biospecimen_data_files.py

echo ./package_root/extract/ctdc/scripts/040_fileOverview.py
./package_root/extract/ctdc/scripts/040_fileOverview.py

echo ./package_root/extract/ctdc/scripts/041_participantAndBiospecimenFilesByStudyId.py
./package_root/extract/ctdc/scripts/041_participantAndBiospecimenFilesByStudyId.py

echo ./package_root/extract/ctdc/scripts/050_publicationInfo.py
./package_root/extract/ctdc/scripts/050_publicationInfo.py

echo ./package_root/extract/ctdc/scripts/060_clinicalData.py
./package_root/extract/ctdc/scripts/060_clinicalData.py

echo ./package_root/extract/ctdc/scripts/061_clinicalTrialData.py
./package_root/extract/ctdc/scripts/061_clinicalTrialData.py

echo ./package_root/extract/ctdc/scripts/100_getInteropData.getAllStudies.py
./package_root/extract/ctdc/scripts/100_getInteropData.getAllStudies.py


