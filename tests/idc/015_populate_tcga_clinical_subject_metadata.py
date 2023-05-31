#!/usr/bin/env python3

from etl.transform.idc.idc_transformer import IDC_transformer

idc = IDC_transformer( source_version = 'v13' )

idc.populate_tcga_clinical_subject_metadata()

