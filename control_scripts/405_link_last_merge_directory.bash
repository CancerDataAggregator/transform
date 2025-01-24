#!/usr/bin/env bash

echo rm -f ./cda_tsvs/last_merge
rm -f ./cda_tsvs/last_merge

echo ln -s merged_icdc_cds_idc_gdc_and_pdc_tables ./cda_tsvs/last_merge
ln -s merged_icdc_cds_idc_gdc_and_pdc_tables ./cda_tsvs/last_merge


