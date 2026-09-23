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

# Harmonize values according to (a) universal 'delete everywhere' maps (nullifying
# values like 'other' and 'not reported') and (b) anything present
# in ./harmonization_maps/ and indexed in 000_cda_column_targets.tsv in that directory.
input_root=./cda_tsvs/ctdc_000_unharmonized
output_root=./cda_tsvs/ctdc_001_harmonized
log_dir=./auxiliary_metadata/__harmonization_logs/ctdc
echo ./package_root/auxiliary_scripts/990_harmonize_cda_tsvs.py $input_root $output_root $log_dir
./package_root/auxiliary_scripts/990_harmonize_cda_tsvs.py $input_root $output_root $log_dir

# Compute aliases; replace ids with aliases wherever needed; create and populate data_at_* and data_source_count columns
last_merge_dir=./cda_tsvs/merged_gdc_pdc_gc_icdc_and_idc_002_decorated_harmonized
input_root=./cda_tsvs/ctdc_001_harmonized
output_root=./cda_tsvs/ctdc_002_decorated_harmonized
echo ./package_root/auxiliary_scripts/991_decorate_harmonized_cda_tsvs_with_aliases_and_provenance.py CTDC $last_merge_dir $input_root $output_root
./package_root/auxiliary_scripts/991_decorate_harmonized_cda_tsvs_with_aliases_and_provenance.py CTDC $last_merge_dir $input_root $output_root


