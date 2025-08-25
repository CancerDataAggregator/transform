#!/usr/bin/env bash

chmod 755 ./package_root/aggregate/phase_800_postproc_final_aggregated_data/scripts/*py

./package_root/aggregate/phase_800_postproc_final_aggregated_data/scripts/000_update_derived_age_values.py


