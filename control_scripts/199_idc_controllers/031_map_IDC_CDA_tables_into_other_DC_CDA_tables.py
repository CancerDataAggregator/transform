#!/usr/bin/env python3 -u

from cda_etl.aggregate.phase_002_merge_idc_into_gdc_and_pdc.idc_aggregator import IDC_aggregator

idc = IDC_aggregator( source_version = 'v17' )

idc.merge_into_existing_cda_data()


