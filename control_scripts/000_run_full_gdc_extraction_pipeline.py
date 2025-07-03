#!/usr/bin/env python3

from cda_etl.extract.gdc.gdc_extractor import GDC_extractor

endpoint_list = [ 'files', 'cases', 'projects', 'annotations' ]

for endpoint in endpoint_list:
    
    extractor = GDC_extractor( endpoint )

    extractor.extract()

print( "\nDon't forget to ensure that a harmonization_maps/ directory is in place before running the next script." )


