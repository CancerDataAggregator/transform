#!/usr/bin/env python3 -u

from cda_etl.aggregate.phase_004_merge_icdc_into_cds_and_idc_and_gdc_and_pdc.icdc_aggregator import ICDC_aggregator

icdc = ICDC_aggregator()

icdc.match_icdc_subject_ids_with_other_dcs()

icdc.merge_into_existing_cda_data()


