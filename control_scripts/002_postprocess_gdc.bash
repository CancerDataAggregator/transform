#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*py

echo ./package_root/auxiliary_scripts/101_count_distinct_values_by_table_and_column.all_extracted_GDC_data.py
./package_root/auxiliary_scripts/101_count_distinct_values_by_table_and_column.all_extracted_GDC_data.py

echo ./package_root/auxiliary_scripts/102_count_distinct_values_by_table_and_column.GDC_CDA_data.py
./package_root/auxiliary_scripts/102_count_distinct_values_by_table_and_column.GDC_CDA_data.py

echo ./package_root/auxiliary_scripts/103_summarize_GDC_entities_by_program_name_and_project_submitter_id.py
./package_root/auxiliary_scripts/103_summarize_GDC_entities_by_program_name_and_project_submitter_id.py

echo ./package_root/auxiliary_scripts/104_enumerate_gdc_program_project_hierarchy.py
./package_root/auxiliary_scripts/104_enumerate_gdc_program_project_hierarchy.py

# The postprocessed output of the following script does not go into CDA public releases; we prepare
# it upstream of ISB-CGC's analytic consumption of the data as a courtesy, but responsibility
# for this level of curation rests firmly with the CRDC data centers and not with CDA.

echo ./package_root/auxiliary_scripts/105_rewire_and_fix_GDC_subsample_provenance.py
./package_root/auxiliary_scripts/105_rewire_and_fix_GDC_subsample_provenance.py


