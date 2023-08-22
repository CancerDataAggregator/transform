#!/usr/bin/env bash

rm -rf cda_tsvs/pdc

chmod 755 ./package_root/transform/pdc/scripts/phase_002_convert_to_cda/*py

echo ./package_root/transform/pdc/scripts/phase_002_convert_to_cda/001_File.py
./package_root/transform/pdc/scripts/phase_002_convert_to_cda/001_File.py

echo ./package_root/transform/pdc/scripts/phase_002_convert_to_cda/002_ResearchSubject.Diagnosis.Specimen.Subject.py
./package_root/transform/pdc/scripts/phase_002_convert_to_cda/002_ResearchSubject.Diagnosis.Specimen.Subject.py


