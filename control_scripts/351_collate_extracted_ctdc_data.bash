#!/usr/bin/env bash

chmod 755 ./package_root/transform/ctdc/scripts/phase_001_collate_and_filter/*py

echo ./package_root/transform/ctdc/scripts/phase_001_collate_and_filter/001_Study.py
./package_root/transform/ctdc/scripts/phase_001_collate_and_filter/001_Study.py

echo ./package_root/transform/ctdc/scripts/phase_001_collate_and_filter/002_DataFile.py
./package_root/transform/ctdc/scripts/phase_001_collate_and_filter/002_DataFile.py

echo ./package_root/transform/ctdc/scripts/phase_001_collate_and_filter/003_Specimen.py
./package_root/transform/ctdc/scripts/phase_001_collate_and_filter/003_Specimen.py

echo ./package_root/transform/ctdc/scripts/phase_001_collate_and_filter/004_Participant_and_subunit_records.py
./package_root/transform/ctdc/scripts/phase_001_collate_and_filter/004_Participant_and_subunit_records.py

echo cp ./extracted_data/ctdc/extraction_date.txt ./extracted_data/ctdc_postprocessed/extraction_date.txt
cp ./extracted_data/ctdc/extraction_date.txt ./extracted_data/ctdc_postprocessed/extraction_date.txt


