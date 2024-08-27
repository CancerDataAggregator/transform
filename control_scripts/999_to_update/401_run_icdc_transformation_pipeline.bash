#!/usr/bin/env bash

chmod 755 package_root/transform/icdc/scripts/*.py ./package_root/auxiliary_scripts/*py

echo ./package_root/transform/icdc/scripts/001_File.py
./package_root/transform/icdc/scripts/001_File.py

echo ./package_root/transform/icdc/scripts/002_ResearchSubject.Diagnosis.Specimen.Subject.py
./package_root/transform/icdc/scripts/002_ResearchSubject.Diagnosis.Specimen.Subject.py

echo ./package_root/auxiliary_scripts/999_harmonize_cda_tsvs.py ./cda_tsvs/icdc_raw_unharmonized ./cda_tsvs/icdc ./auxiliary_metadata/__substitution_logs/icdc
./package_root/auxiliary_scripts/999_harmonize_cda_tsvs.py ./cda_tsvs/icdc_raw_unharmonized ./cda_tsvs/icdc ./auxiliary_metadata/__substitution_logs/icdc


