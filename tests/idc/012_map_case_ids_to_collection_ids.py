#!/usr/bin/env python3

from etl.transform.idc.idc_transformer import IDC_transformer

idc = IDC_transformer( source_version = 'v13' )

idc.map_case_ids_to_collection_ids()


