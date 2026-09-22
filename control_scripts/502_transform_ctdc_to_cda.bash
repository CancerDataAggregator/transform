#!/usr/bin/env bash

chmod 755 ./package_root/transform/ctdc/scripts/phase_002_convert_to_cda/*py

echo ./package_root/transform/ctdc/scripts/phase_002_convert_to_cda/001_study.py
./package_root/transform/ctdc/scripts/phase_002_convert_to_cda/001_study.py

echo ./package_root/transform/ctdc/scripts/phase_002_convert_to_cda/002_file.py
./package_root/transform/ctdc/scripts/phase_002_convert_to_cda/002_file.py

echo ./package_root/transform/ctdc/scripts/phase_002_convert_to_cda/003_subject.py
./package_root/transform/ctdc/scripts/phase_002_convert_to_cda/003_subject.py

echo ./package_root/transform/ctdc/scripts/phase_002_convert_to_cda/004_file_describes_subject.py
./package_root/transform/ctdc/scripts/phase_002_convert_to_cda/004_file_describes_subject.py

echo ./package_root/transform/ctdc/scripts/phase_002_convert_to_cda/005_observation.py
./package_root/transform/ctdc/scripts/phase_002_convert_to_cda/005_observation.py

echo ./package_root/transform/ctdc/scripts/phase_002_convert_to_cda/006_treatment.py
./package_root/transform/ctdc/scripts/phase_002_convert_to_cda/006_treatment.py


