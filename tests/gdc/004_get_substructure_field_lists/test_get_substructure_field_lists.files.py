#!/usr/bin/env python

from etl.extract.gdc.gdc_extractor import GDC_extractor

for endpoint in [ 'files' ]:
    
    extractor = GDC_extractor(endpoint, refresh=False)

    extractor.get_field_lists_for_API_calls()

    extractor.get_endpoint_records()

    extractor.make_base_table()

    extractor.set_refresh(True)

    extractor.get_substructure_field_lists()


