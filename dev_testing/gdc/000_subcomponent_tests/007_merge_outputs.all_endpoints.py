#!/usr/bin/env python

from cda_etl.extract.gdc.gdc_extractor import GDC_extractor

for endpoint in [ 'files', 'cases', 'projects', 'annotations' ]:
    
    extractor = GDC_extractor( endpoint, refresh=False )

    extractor.extract()

    extractor.set_refresh(True)

    extractor.update_merged_output_directory()


