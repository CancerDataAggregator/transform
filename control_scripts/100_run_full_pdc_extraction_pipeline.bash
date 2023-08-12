#!/bin/bash

chmod 755 ./package_root/extract/pdc/scripts/*py

echo ./package_root/extract/pdc/scripts/001_uiDataVersionSoftwareVersion.py
./package_root/extract/pdc/scripts/001_uiDataVersionSoftwareVersion.py

echo ./package_root/extract/pdc/scripts/010_allPrograms.py
./package_root/extract/pdc/scripts/010_allPrograms.py

echo ./package_root/extract/pdc/scripts/011_projectsPerExperimentType.py
./package_root/extract/pdc/scripts/011_projectsPerExperimentType.py

echo ./package_root/extract/pdc/scripts/012_studyCatalog.py
./package_root/extract/pdc/scripts/012_studyCatalog.py

echo ./package_root/extract/pdc/scripts/013_getPaginatedUIStudy.py
./package_root/extract/pdc/scripts/013_getPaginatedUIStudy.py

echo ./package_root/extract/pdc/scripts/014_uiLegacyStudies.py
./package_root/extract/pdc/scripts/014_uiLegacyStudies.py

echo ./package_root/extract/pdc/scripts/015_uiHeatmapStudies.py
./package_root/extract/pdc/scripts/015_uiHeatmapStudies.py

echo ./package_root/extract/pdc/scripts/020_getPaginatedFiles.py
./package_root/extract/pdc/scripts/020_getPaginatedFiles.py

echo ./package_root/extract/pdc/scripts/021_fileMetadata.py
./package_root/extract/pdc/scripts/021_fileMetadata.py

echo ./package_root/extract/pdc/scripts/022_getPaginatedUIFile.py
./package_root/extract/pdc/scripts/022_getPaginatedUIFile.py

echo ./package_root/extract/pdc/scripts/023_filesPerStudy.py
./package_root/extract/pdc/scripts/023_filesPerStudy.py

echo ./package_root/extract/pdc/scripts/030_case.py
./package_root/extract/pdc/scripts/030_case.py

echo ./package_root/extract/pdc/scripts/033_paginatedCasesSamplesAliquots.py
./package_root/extract/pdc/scripts/033_paginatedCasesSamplesAliquots.py

echo ./package_root/extract/pdc/scripts/040_protocolPerStudy.py
./package_root/extract/pdc/scripts/040_protocolPerStudy.py

echo ./package_root/extract/pdc/scripts/041_uiProtocol.per_pdc_study_id.py
./package_root/extract/pdc/scripts/041_uiProtocol.per_pdc_study_id.py

echo ./package_root/extract/pdc/scripts/050_uiPublication.py
./package_root/extract/pdc/scripts/050_uiPublication.py

echo ./package_root/extract/pdc/scripts/060_workflowMetadata.py
./package_root/extract/pdc/scripts/060_workflowMetadata.py

echo ./package_root/extract/pdc/scripts/061_experimentalMetadata.py
./package_root/extract/pdc/scripts/061_experimentalMetadata.py

echo ./package_root/extract/pdc/scripts/062_studyExperimentalDesign.py
./package_root/extract/pdc/scripts/062_studyExperimentalDesign.py

echo ./package_root/extract/pdc/scripts/070_biospecimenPerStudy.py
./package_root/extract/pdc/scripts/070_biospecimenPerStudy.py

echo ./package_root/extract/pdc/scripts/071_clinicalMetadata.py
./package_root/extract/pdc/scripts/071_clinicalMetadata.py

echo ./package_root/extract/pdc/scripts/080_getPaginatedGenes.py
./package_root/extract/pdc/scripts/080_getPaginatedGenes.py

echo ./package_root/extract/pdc/scripts/090_reference.py
./package_root/extract/pdc/scripts/090_reference.py

echo ./package_root/extract/pdc/scripts/100_uiPrimarySiteCaseCount.py
./package_root/extract/pdc/scripts/100_uiPrimarySiteCaseCount.py

echo ./package_root/extract/pdc/scripts/101_allExperimentTypes.py
./package_root/extract/pdc/scripts/101_allExperimentTypes.py

echo 'all done! Don\'t forget to \"grep error \*\" all the API JSON results to make sure nothing server-side crashed partway through the extraction run.'


