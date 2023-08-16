#!/usr/bin/env python

from cda_etl.extract.gdc.gdc_extractor import GDC_extractor

extractor = GDC_extractor( 'files', refresh=False )

# refresh == False: do nothing

extractor.save_API_status_endpoint()

# refresh == False: load field lists cached in previously written metadata files

extractor.get_field_lists_for_API_calls()

# refresh == False: do nothing

extractor.get_endpoint_records()

# refresh == False: do nothing

extractor.make_base_table()

# refresh == False: retrieve field lists from API but don't overwrite previously written field list metadata files

extractor.get_substructure_field_lists()

# refresh == False: do nothing

extractor.make_substructure_tables()

# refresh == False: do nothing

extractor.make_association_tables()

# refresh == False: do nothing

###########################
### set refresh = True
###########################
# 
extractor.set_refresh(True)
# 
###########################

extractor.update_merged_output_directory()





