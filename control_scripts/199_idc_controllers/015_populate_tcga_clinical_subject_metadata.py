#!/usr/bin/env python3 -u

from cda_etl.transform.idc.idc_transformer import IDC_transformer

idc = IDC_transformer( source_version = 'v17' )

idc.populate_tcga_clinical_subject_metadata()


