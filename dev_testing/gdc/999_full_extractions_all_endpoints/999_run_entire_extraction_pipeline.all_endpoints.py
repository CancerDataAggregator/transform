#!/usr/bin/env python

from etl.extract.gdc.gdc_extractor import GDC_extractor

endpoint_list = [ 'files', 'cases', 'projects', 'annotations' ]

for endpoint in endpoint_list:
    
    extractor = GDC_extractor(endpoint)

    extractor.extract()


