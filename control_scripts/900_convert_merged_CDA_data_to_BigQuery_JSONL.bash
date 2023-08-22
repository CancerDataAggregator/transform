#!/usr/bin/env bash

chmod 755 ./199_idc_controllers/*py

echo ./199_idc_controllers/040_convert_subjects_and_sub_objects_to_JSONL.py
./199_idc_controllers/040_convert_subjects_and_sub_objects_to_JSONL.py

echo ./199_idc_controllers/041_convert_files_to_JSONL.py
./199_idc_controllers/041_convert_files_to_JSONL.py


