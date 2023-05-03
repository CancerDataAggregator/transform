#!/usr/bin/env python3

from etl.aggregate.step_002_merge_idc_into_gdc_and_pdc.idc_aggregator import IDC_aggregator

idc = IDC_aggregator( source_version = 'v13' )

idc.match_idc_subject_ids_with_other_dcs()


