#!/usr/bin/env python3

from cda_etl.transform.idc.idc_transformer import IDC_transformer

idc = IDC_transformer( source_version = 'v15' )

idc.extract_submitter_case_IDs_from_auxiliary_metadata()


