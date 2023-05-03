#!/usr/bin/env python3

from etl.extract.gdc.gdc_extractor import GDC_extractor

endpoint_list = [ 'files', 'cases', 'projects', 'annotations' ]

for endpoint in endpoint_list:
    
    extractor = GDC_extractor(endpoint)

    extractor.save_API_status_endpoint()


