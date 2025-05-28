#!/usr/bin/env bash

chmod 755 ./package_root/transform/pdc/scripts/phase_002_convert_to_cda/*py ./package_root/auxiliary_scripts/*py

echo ./package_root/transform/pdc/scripts/phase_002_convert_to_cda/001_program_and_project.py
./package_root/transform/pdc/scripts/phase_002_convert_to_cda/001_program_and_project.py

echo ./package_root/transform/pdc/scripts/phase_002_convert_to_cda/002_file.py
./package_root/transform/pdc/scripts/phase_002_convert_to_cda/002_file.py

echo ./package_root/transform/pdc/scripts/phase_002_convert_to_cda/003_subject.py
./package_root/transform/pdc/scripts/phase_002_convert_to_cda/003_subject.py

echo ./package_root/transform/pdc/scripts/phase_002_convert_to_cda/004_file_describes_subject.py
./package_root/transform/pdc/scripts/phase_002_convert_to_cda/004_file_describes_subject.py

echo ./package_root/transform/pdc/scripts/phase_002_convert_to_cda/005_observation.py
./package_root/transform/pdc/scripts/phase_002_convert_to_cda/005_observation.py

echo ./package_root/transform/pdc/scripts/phase_002_convert_to_cda/006_treatment.py
./package_root/transform/pdc/scripts/phase_002_convert_to_cda/006_treatment.py

# Harmonize values according to (a) universal 'delete everywhere' maps (nullifying
# values like 'other' and 'not reported') and (b) anything present
# in ./harmonization_maps/ and indexed in 000_cda_column_targets.tsv in that directory.

echo ./package_root/auxiliary_scripts/990_harmonize_cda_tsvs.py ./cda_tsvs/pdc_000_unharmonized ./cda_tsvs/pdc_001_harmonized ./auxiliary_metadata/__harmonization_logs/pdc
./package_root/auxiliary_scripts/990_harmonize_cda_tsvs.py ./cda_tsvs/pdc_000_unharmonized ./cda_tsvs/pdc_001_harmonized ./auxiliary_metadata/__harmonization_logs/pdc

# Compute aliases; replace ids with aliases wherever needed; create and populate data_at_* and data_source_count columns

echo ./package_root/auxiliary_scripts/991_decorate_harmonized_cda_tsvs_with_aliases_and_provenance.py PDC ./cda_tsvs/gdc_002_decorated_harmonized ./cda_tsvs/pdc_001_harmonized ./cda_tsvs/pdc_002_decorated_harmonized
./package_root/auxiliary_scripts/991_decorate_harmonized_cda_tsvs_with_aliases_and_provenance.py PDC ./cda_tsvs/gdc_002_decorated_harmonized ./cda_tsvs/pdc_001_harmonized ./cda_tsvs/pdc_002_decorated_harmonized


