#!/usr/bin/env python3

from etl.transform.idc.idc_transformer import IDC_transformer

idc = IDC_transformer( source_version = 'v13' )

idc.extract_specimen_data_from_tcga_biospecimen_rel9()


