#!/usr/bin/env python3 -u

import gzip
import jsonlines
import sys

from os import path, makedirs

from cda_etl.lib import get_current_timestamp

# PARAMETERS

extraction_root = path.join( 'extracted_data', 'idc' )

jsonl_input_dir = path.join( extraction_root, '__raw_BigQuery_JSONL' )

table_name = 'dicom_all'

input_file = path.join( jsonl_input_dir, f"{table_name}.jsonl.gz" )

output_dir = extraction_root

output_file = path.join( output_dir, f"dicom_instance.tsv.gz" )

instance_metadata_fields = [
    
    'SOPInstanceUID',
    'crdc_series_uuid',
    'gcs_url',
    'crdc_instance_uuid',
    'instance_size',
    'instance_hash',
    'SOPClassUID'
]

# EXECUTION

for target_dir in [ output_dir ]:
    
    if not path.isdir( output_dir ):
        
        makedirs( output_dir )

# Load DICOM instance metadata from dicom_all, process, and dump directly to our output TSV.

print( f"[{get_current_timestamp()}] Scanning {table_name} for instance metadata and writing to {output_file}...", end='\n\n', file=sys.stderr )

line_count = 0

display_increment = 5000000

with gzip.open( input_file ) as IN, gzip.open( output_file, 'wt' ) as OUT:
    
    print( *instance_metadata_fields, sep='\t', file=OUT )

    reader = jsonlines.Reader( IN )

    for record in reader:
        
        if 'SOPInstanceUID' not in record or record['SOPInstanceUID'] is None or record['SOPInstanceUID'] == '':
            
            sys.exit( f"FATAL: Record at line {line_count + 1} has no SOPInstanceUID; assumptions violated, cannot continue." )

        output_row = list()

        for field_name in instance_metadata_fields:
            
            if field_name in record and record[field_name] is not None and record[field_name] != '':
                
                output_row.append( record[field_name] )

            else:
                
                output_row.append( '' )

        print( *output_row, sep='\t', file=OUT )

        line_count = line_count + 1

        if line_count % display_increment == 0:
            
            print( f"    [{get_current_timestamp()}] ...scanned {line_count} lines...", file=sys.stderr )

print( f"\n[{get_current_timestamp()}] ...done.", file=sys.stderr )


