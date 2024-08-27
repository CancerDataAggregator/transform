#!/usr/bin/env python3 -u

import sys

from cda_etl.aggregate.phase_002_merge_idc_into_gdc_and_pdc.idc_aggregator import IDC_aggregator

version_string = sys.argv[1]

idc = IDC_aggregator( source_version = version_string )

idc.match_idc_subject_ids_with_other_dcs()


