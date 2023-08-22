#!/usr/bin/env bash

chmod 755 ./199_idc_controllers/*py

echo ./199_idc_controllers/030_map_IDC_CDA_subject_IDs_to_other_DC_CDA_subject_ids.py
./199_idc_controllers/030_map_IDC_CDA_subject_IDs_to_other_DC_CDA_subject_ids.py

echo ./199_idc_controllers/031_map_IDC_CDA_tables_into_other_DC_CDA_tables.py
./199_idc_controllers/031_map_IDC_CDA_tables_into_other_DC_CDA_tables.py


