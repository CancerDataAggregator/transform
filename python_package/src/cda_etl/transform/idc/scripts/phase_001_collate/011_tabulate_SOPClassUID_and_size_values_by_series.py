#!/usr/bin/env python3 -u

import gzip
import sys

from os import path, makedirs

from cda_etl.lib import get_current_timestamp

# PARAMETERS

extraction_root = path.join( 'extracted_data', 'idc' )

input_file = path.join( extraction_root, 'dicom_instance.tsv.gz' )

class_output_file = path.join( extraction_root, 'dicom_series.dicom_all.SOPClassUID.tsv' )

size_output_file = path.join( extraction_root, 'dicom_series.dicom_all.instance_size.tsv' )

# EXECUTION

print( f"\n[{get_current_timestamp()}] Scanning {input_file} for SOPClassUID values and aggregating by series...", end='\n\n', file=sys.stderr )

line_count = 0

display_increment = 5000000

series_classes = dict()

series_sizes = dict()

with gzip.open( input_file , 'rt' ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in IN:
        
        record = dict( zip( column_names, line.rstrip( '\n' ).split( '\t' ) ) )

        series_id = record['crdc_series_uuid']

        class_uid = ''

        if 'SOPClassUID' in record and record['SOPClassUID'] is not None and record['SOPClassUID'] != '':
            
            class_uid = record['SOPClassUID']

        if series_id not in series_classes:
            
            series_classes[series_id] = dict()

        if class_uid not in series_classes[series_id]:
            
            series_classes[series_id][class_uid] = 1

        else:
            
            series_classes[series_id][class_uid] = series_classes[series_id][class_uid] + 1

        size_val = ''

        if 'instance_size' in record and record['instance_size'] is not None and record['instance_size'] != '':
            
            size_val = record['instance_size']

        if series_id not in series_sizes:
            
            series_sizes[series_id] = dict()

        if size_val not in series_sizes[series_id]:
            
            series_sizes[series_id][size_val] = 1

        else:
            
            series_sizes[series_id][size_val] = series_sizes[series_id][size_val] + 1

        line_count = line_count + 1

        if line_count % display_increment == 0:
            
            print( f"    [{get_current_timestamp()}] ...scanned {line_count} lines...", file=sys.stderr )

print( f"\n[{get_current_timestamp()}] ...done.", end='\n\n', file=sys.stderr )

print( f"[{get_current_timestamp()}] Writing {class_output_file}...", end='', file=sys.stderr )

# Write SOPClassUID output TSV.

with open( class_output_file, 'w' ) as OUT:
    
    print( *[ 'crdc_series_uuid', 'SOPClassUID', 'number_of_matching_SOPInstanceUIDs' ], sep='\t', file=OUT )

    for series_id in sorted( series_classes ):
        
        for class_uid in sorted( series_classes[series_id] ):
            
            print( *[ series_id, class_uid, series_classes[series_id][class_uid] ], sep='\t', file=OUT )

print( 'done.', end='\n\n', file=sys.stderr )

print( f"[{get_current_timestamp()}] Writing {size_output_file}...", end='', file=sys.stderr )

# Write instance_size output TSV.

with open( size_output_file, 'w' ) as OUT:
    
    print( *[ 'crdc_series_uuid', 'instance_size', 'number_of_matching_SOPInstanceUIDs' ], sep='\t', file=OUT )

    for series_id in sorted( series_sizes ):
        
        for size_val in sorted( series_sizes[series_id] ):
            
            print( *[ series_id, size_val, series_sizes[series_id][size_val] ], sep='\t', file=OUT )

print( 'done.', end='\n\n', file=sys.stderr )


