#!/usr/bin/env python3 -u

import gzip
import jsonlines

from os import path, makedirs

from cda_etl.lib import get_idc_extraction_fields

# PARAMETERS

extraction_root = path.join( 'extracted_data', 'idc' )

jsonl_input_dir = path.join( extraction_root, '__raw_BigQuery_JSONL' )

table_name = 'original_collections_metadata'

input_file = path.join( jsonl_input_dir, f"{table_name}.jsonl.gz" )

toplevel_extraction_fields = get_idc_extraction_fields()[table_name]

output_dir = extraction_root

toplevel_output_file = path.join( output_dir, f"{table_name}.tsv" )

# EXECUTION

if not path.isdir( output_dir ):
    
    makedirs( output_dir )

with gzip.open( input_file ) as IN, open( toplevel_output_file, 'w' ) as OUT:
    
    print( *toplevel_extraction_fields, sep='\t', file=OUT )

    reader = jsonlines.Reader( IN )

    for record in reader:
        
        output_row = list()

        for field_name in toplevel_extraction_fields:
            
            if field_name in record and record[field_name] is not None:
                
                output_row.append( record[field_name] )

            else:
                
                output_row.append( '' )

        print( *output_row, sep='\t', file=OUT )


