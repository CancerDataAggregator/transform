#!/usr/bin/env bash

rm -rf cda_tsvs/pdc

chmod 755 ./package_root/transform/pdc/scripts/phase_002_convert_to_cda/*py ./package_root/auxiliary_scripts/*py

echo ./package_root/transform/pdc/scripts/phase_002_convert_to_cda/001_File.py
./package_root/transform/pdc/scripts/phase_002_convert_to_cda/001_File.py

echo ./package_root/transform/pdc/scripts/phase_002_convert_to_cda/002_ResearchSubject.Diagnosis.Specimen.Subject.py
./package_root/transform/pdc/scripts/phase_002_convert_to_cda/002_ResearchSubject.Diagnosis.Specimen.Subject.py

echo ./package_root/auxiliary_scripts/999_harmonize_cda_tsvs.py ./cda_tsvs/pdc_raw_unharmonized ./cda_tsvs/pdc ./auxiliary_metadata/__substitution_logs/pdc
./package_root/auxiliary_scripts/999_harmonize_cda_tsvs.py ./cda_tsvs/pdc_raw_unharmonized ./cda_tsvs/pdc ./auxiliary_metadata/__substitution_logs/pdc


