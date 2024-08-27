#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*py ./package_root/auxiliary_scripts/*pl

echo ./package_root/auxiliary_scripts/100_list_PDC_entities_by_program_project_and_study.py
./package_root/auxiliary_scripts/100_list_PDC_entities_by_program_project_and_study.py

echo ./package_root/auxiliary_scripts/101_enumerate_PDC_program_project_study_hierarchy.py
./package_root/auxiliary_scripts/101_enumerate_PDC_program_project_study_hierarchy.py

echo ./package_root/auxiliary_scripts/110_draft_PDC_GDC_project_links_using_submitter_IDs.pl
./package_root/auxiliary_scripts/110_draft_PDC_GDC_project_links_using_submitter_IDs.pl

>&2 echo

>&2 echo "Don't forget to create 'naive_GDC_PDC_project_id_map.hand_edited_to_remove_false_positives.tsv' before proceeding!"

>&2 echo


