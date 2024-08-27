#!/usr/bin/env bash

chmod 755 ./package_root/transform/gdc/scripts/*py ./package_root/auxiliary_scripts/*py

echo ./package_root/transform/gdc/scripts/001_program_and_project.py
./package_root/transform/gdc/scripts/001_program_and_project.py

echo ./package_root/transform/gdc/scripts/002_file_and_subject.py
./package_root/transform/gdc/scripts/002_file_and_subject.py

echo ./package_root/transform/gdc/scripts/003_observation_and_treatment.py
./package_root/transform/gdc/scripts/003_observation_and_treatment.py

# Harmonize values according to (a) universal 'delete everywhere' maps (nullifying
# values like 'other' and 'not reported') and (b) anything present
# in ./harmonization_maps/ and indexed in 000_cda_column_targets.tsv in that directory.

echo ./package_root/auxiliary_scripts/990_harmonize_cda_tsvs.py ./cda_tsvs/gdc_000_unharmonized ./cda_tsvs/gdc_001_harmonized ./auxiliary_metadata/__harmonization_logs/gdc
./package_root/auxiliary_scripts/990_harmonize_cda_tsvs.py ./cda_tsvs/gdc_000_unharmonized ./cda_tsvs/gdc_001_harmonized ./auxiliary_metadata/__harmonization_logs/gdc

# Compute aliases; replace ids with aliases wherever needed; create and populate data_at_* and data_source_count columns

echo ./package_root/auxiliary_scripts/991_decorate_harmonized_cda_tsvs_with_aliases_and_provenance.py GDC NO_ANTECEDENT_MERGE_DIR ./cda_tsvs/gdc_001_harmonized ./cda_tsvs/gdc_002_decorated_harmonized
./package_root/auxiliary_scripts/991_decorate_harmonized_cda_tsvs_with_aliases_and_provenance.py GDC NO_ANTECEDENT_MERGE_DIR ./cda_tsvs/gdc_001_harmonized ./cda_tsvs/gdc_002_decorated_harmonized

# Link final data product with a rolling symlink to indicate the input dir for the next aggregation pass.

echo rm -f ./cda_tsvs/last_merge
rm -f ./cda_tsvs/last_merge

echo ln -sf gdc_002_decorated_harmonized ./cda_tsvs/last_merge
ln -sf gdc_002_decorated_harmonized ./cda_tsvs/last_merge


