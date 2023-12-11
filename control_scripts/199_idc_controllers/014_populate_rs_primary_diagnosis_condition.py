#!/usr/bin/env python3 -u

from cda_etl.transform.idc.idc_transformer import IDC_transformer

idc = IDC_transformer( source_version = 'v16' )

idc.populate_rs_primary_diagnosis_condition()


