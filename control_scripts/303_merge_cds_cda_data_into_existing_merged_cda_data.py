#!/usr/bin/env python3 -u

from cda_etl.aggregate.phase_003_merge_cds_into_idc_and_gdc_and_pdc.cds_aggregator import CDS_aggregator

cds = CDS_aggregator()

cds.match_cds_subject_ids_with_other_dcs()

cds.merge_into_existing_cda_data()


