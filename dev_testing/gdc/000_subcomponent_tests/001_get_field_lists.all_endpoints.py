#!/usr/bin/env python

from cda_etl.extract.gdc.gdc_extractor import GDC_extractor

for endpoint in [ 'annotations', 'cases', 'files', 'projects' ]:
    
    extractor = GDC_extractor(endpoint)

    extractor.get_field_lists_for_API_calls()

