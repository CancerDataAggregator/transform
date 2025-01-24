#!/usr/bin/env bash

chmod 755 ./package_root/extract/pdc/scripts/*py

( echo ./package_root/extract/pdc/scripts/001_uiDataVersionSoftwareVersion.py && \
  ( ( ./package_root/extract/pdc/scripts/001_uiDataVersionSoftwareVersion.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/001_uiDataVersionSoftwareVersion.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/010_allPrograms.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/010_allPrograms.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/010_allPrograms.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/011_projectsPerExperimentType.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/011_projectsPerExperimentType.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/011_projectsPerExperimentType.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/012_studyCatalog.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/012_studyCatalog.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/012_studyCatalog.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/013_getPaginatedUIStudy.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/013_getPaginatedUIStudy.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/013_getPaginatedUIStudy.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/014_uiLegacyStudies.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/014_uiLegacyStudies.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/014_uiLegacyStudies.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/015_uiHeatmapStudies.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/015_uiHeatmapStudies.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/015_uiHeatmapStudies.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/020_getPaginatedFiles.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/020_getPaginatedFiles.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/020_getPaginatedFiles.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/021_fileMetadata.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/021_fileMetadata.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/021_fileMetadata.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/022_getPaginatedUIFile.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/022_getPaginatedUIFile.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/022_getPaginatedUIFile.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/023_filesPerStudy.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/023_filesPerStudy.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/023_filesPerStudy.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/030_case_with_diagnoses.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/030_case_with_diagnoses.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/030_case_with_diagnoses.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/031_case_with_demographics.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/031_case_with_demographics.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/031_case_with_demographics.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/032_case_with_samples.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/032_case_with_samples.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/032_case_with_samples.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/038_case_with_the_rest.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/038_case_with_the_rest.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/038_case_with_the_rest.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/039_paginatedCasesSamplesAliquots.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/039_paginatedCasesSamplesAliquots.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/039_paginatedCasesSamplesAliquots.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/040_protocolPerStudy.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/040_protocolPerStudy.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/040_protocolPerStudy.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/041_uiProtocol.per_pdc_study_id.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/041_uiProtocol.per_pdc_study_id.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/041_uiProtocol.per_pdc_study_id.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/050_uiPublication.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/050_uiPublication.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/050_uiPublication.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/060_workflowMetadata.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/060_workflowMetadata.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/060_workflowMetadata.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/061_experimentalMetadata.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/061_experimentalMetadata.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/061_experimentalMetadata.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/062_studyExperimentalDesign.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/062_studyExperimentalDesign.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/062_studyExperimentalDesign.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/070_biospecimenPerStudy.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/070_biospecimenPerStudy.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/070_biospecimenPerStudy.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/071_clinicalMetadata.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/071_clinicalMetadata.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/071_clinicalMetadata.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/080_getPaginatedGenes.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/080_getPaginatedGenes.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/080_getPaginatedGenes.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/090_reference.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/090_reference.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/090_reference.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/100_uiPrimarySiteCaseCount.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/100_uiPrimarySiteCaseCount.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/100_uiPrimarySiteCaseCount.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) ) && \
( echo ./package_root/extract/pdc/scripts/101_allExperimentTypes.py && \
  ( ( sleep 10; ./package_root/extract/pdc/scripts/101_allExperimentTypes.py ) || \
      echo "FAILED: ./package_root/extract/pdc/scripts/101_allExperimentTypes.py\n\nTry rerunning the failed script: that often works. Sometimes waiting a few minutes helps." ) )

echo "all done! Don't forget to 'grep error *' all the API JSON results to make sure nothing server-side crashed partway through the extraction run."


