#!/usr/bin/env python

from etl.extract.gdc.gdc_extractor import GDC_extractor

for endpoint in [ 'cases' ]:
    
    extractor = GDC_extractor(endpoint, refresh=False)

    extractor.get_field_lists_for_API_calls()

    extractor.set_refresh(True)

    extractor.get_endpoint_records()


