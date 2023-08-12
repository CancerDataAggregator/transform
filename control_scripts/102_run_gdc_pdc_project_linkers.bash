#!/bin/bash

chmod 755 ./package_root/auxiliary_scripts/*py ./package_root/auxiliary_scripts/*pl

echo ./package_root/auxiliary_scripts/200_summarize_PDC_entities_by_program_project_and_study.py
./package_root/auxiliary_scripts/200_summarize_PDC_entities_by_program_project_and_study.py

echo ./package_root/auxiliary_scripts/201_enumerate_pdc_program_project_study_hierarchy.py
./package_root/auxiliary_scripts/201_enumerate_pdc_program_project_study_hierarchy.py

echo ./package_root/auxiliary_scripts/300_naively_equate_PDC_project_and_study_submitter_IDs_to_GDC_project_IDs.pl
./package_root/auxiliary_scripts/300_naively_equate_PDC_project_and_study_submitter_IDs_to_GDC_project_IDs.pl

>&2 echo "\nDon't forget to hand-edit the project map before proceeding!\n\n";


