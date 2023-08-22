#!/usr/bin/env python3 -u

from cda_etl.transform.idc.idc_transformer import IDC_transformer

idc = IDC_transformer( source_version = 'v15' )

idc.map_case_ids_to_collection_ids()


