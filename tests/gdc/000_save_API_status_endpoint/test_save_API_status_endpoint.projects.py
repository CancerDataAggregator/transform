#!/usr/bin/env python3

from etl.extract.gdc.gdc_extractor import GDC_extractor

endpoint = 'projects'

extractor = GDC_extractor(endpoint)

extractor.save_API_status_endpoint()

